//! An aggregator for executions of crypto-currency exchanges.
//!
//! This tool collaborates with `pikmin`, which is a downloader for execution data, so that
//! reduce data sizes by aggregating rows at the same timestamp.
//!
//! Currently, this only supports MySQL.

use chrono::Utc;
use log::trace;
use mysql::Pool;

/// A struct, which keeps a DB connection, to aggregate data for one exchange.
#[derive(Debug)]
pub struct Aggregator<'a> {
    pool: Pool,
    exchange_name: &'a str,
}

impl<'a> Aggregator<'a> {
    /// Creates an aggregator instance with a DB url and an exchange name (table name).
    pub fn new(url: &str, exchange_name: &'a str) -> Self {
        let pool: Pool = Pool::new(url).expect("cannot connect to MySQL instance");
        Self {
            pool,
            exchange_name,
        }
    }

    /// Run aggregation
    pub fn aggregate(&self) {
        trace!("start aggregate: {}", self.exchange_name);
        trace!(
            "original: {}, target: {}",
            self.original_table(),
            self.target_table()
        );

        if !self.check_existence(&self.target_table()) {
            let step1_table_name = self.get_step1_table_name(&self.original_table());
            if !self.check_existence(&step1_table_name) {
                self.step1(&step1_table_name);
            }

            let step2_table_name = self.get_step2_table_name(&self.original_table());
            if !self.check_existence(&step2_table_name) {
                self.clear_step2_table(&step2_table_name);
            }

            self.step2(&self.original_table(), &step1_table_name, &step2_table_name);
            self.rename_table(&step2_table_name, &self.target_table());
            self.drop_table(&step1_table_name);
        }

        trace!("finish: {}", self.exchange_name);
    }

    fn check_existence(&self, table_name: &str) -> bool {
        let existence_stmt = format!(r"SHOW TABLES LIKE '{}';", table_name);
        let exists = self
            .pool
            .prep_exec(existence_stmt, ())
            .map(|result| result.map(|x| x.unwrap()).count());
        exists.map(|e| e != 0).unwrap_or(false)
    }

    fn drop_table(&self, table_name: &str) {
        let drop_stmt = format!(r"DROP TABLE IF EXISTS {};", table_name);
        self.pool.prep_exec(drop_stmt, ()).unwrap();
    }

    fn rename_table(&self, orig: &str, to: &str) {
        let rename_stmt = format!(r"RENAME TABLE {} TO {};", orig, to);
        self.pool.prep_exec(rename_stmt, ()).unwrap();
    }

    fn get_temporary_table_name(&self, table_name: &str) -> String {
        format!("tmp__{}", table_name)
    }

    fn original_table(&self) -> String {
        format!("trades_{}", self.exchange_name)
    }
    fn target_table(&self) -> String {
        format!("ref_trades_{}", self.exchange_name)
    }

    fn get_step1_table_name(&self, table_name: &str) -> String {
        format!("step1__{}", table_name)
    }

    fn get_step2_table_name(&self, table_name: &str) -> String {
        format!("step2__{}", table_name)
    }

    // faster version, using OUTFILE/LOAD.
    // https://github.com/docker-library/mysql/issues/447
    fn insert_all_traded_at(&self, table_name: &str) -> u64 {
        const LIMIT: u32 = 100000;

        let out_file_name = format!(
            "/tmp/table_{}_{}.txt",
            self.exchange_name,
            Utc::now().timestamp_millis()
        );

        // insert
        {
            // cannot use prepared statement
            let stmt = format!(
                r#"SELECT DISTINCT traded_at
                    FROM {}
                    WHERE traded_at >
                    COALESCE(
                        (SELECT traded_at FROM {} ORDER BY traded_at DESC LIMIT 1),
                        '2000-01-01 00:00:00.000'
                    )
                    ORDER BY traded_at
                    LIMIT {lm}
                    INTO OUTFILE '{of}'
                        ;"#,
                self.original_table(),
                table_name,
                of = out_file_name,
                lm = LIMIT
            );
            trace!("run stmt: {}", stmt);
            self.pool.get_conn().unwrap().query(stmt).unwrap();
        }
        // load
        let result = {
            // cannot use prepared statement
            let stmt = format!(
                r#"LOAD DATA INFILE '{of}' INTO TABLE {};"#,
                table_name,
                of = out_file_name
            );
            trace!("run stmt: {}", stmt);
            self.pool
                .get_conn()
                .unwrap()
                .query(stmt)
                .map(|result| result.affected_rows())
                .unwrap()
        };

        result
    }

    // write all traded_at in the specified term
    fn step1(&self, table_name: &str) {
        let tmp_table_name = self.get_temporary_table_name(&table_name);

        self.drop_table(&tmp_table_name);
        let create_stmt = format!(
            r"CREATE TABLE {} (
                                 traded_at TIMESTAMP(3) NOT NULL PRIMARY KEY
                             );",
            tmp_table_name
        );
        self.pool.prep_exec(create_stmt, ()).unwrap();

        // fetch all traded_at, and insert it
        let mut i = 0;
        loop {
            trace!("[{}] offset: {}", self.exchange_name, i);
            let inserted = self.insert_all_traded_at(&tmp_table_name);
            i += 1;
            if inserted == 0 {
                break;
            }
        }

        self.rename_table(&tmp_table_name, &table_name);
    }

    fn clear_step2_table(&self, s2_table_name: &str) {
        self.drop_table(&s2_table_name);

        let create_stmt = format!(
            r"CREATE TABLE {} (
                         traded_at TIMESTAMP(3) NOT NULL PRIMARY KEY,
                         amount DOUBLE NOT NULL,
                         price FLOAT NOT NULL
                     );",
            s2_table_name
        );
        self.pool.prep_exec(create_stmt, ()).unwrap();
    }

    // TODO: in parallel
    fn move_aggregated_data(
        &self,
        s1_table_name: &str,
        s2_table_name: &str,
        data_table_name: &str,
    ) -> u64 {
        const LIMIT: u32 = 100000;

        trace!("insert_aggregated_data: limit: {}", LIMIT);

        self.pool
            .start_transaction(false, None, None)
            .and_then(|mut tx| {
                let stmt = format!(
                    r#"INSERT INTO {s2} (traded_at,amount,price)
                        SELECT {s1}.traded_at,Sum({orig}.amount),Avg({orig}.price)
                        FROM   {s1}
                        LEFT JOIN {orig} ON {s1}.traded_at = {orig}.traded_at
                        GROUP  BY traded_at
                        LIMIT  ?
                        ;"#,
                    s1 = s1_table_name,
                    s2 = s2_table_name,
                    orig = data_table_name
                );
                trace!("agg_stmt: {}", stmt);
                let t = tx
                    .prep_exec(stmt, (LIMIT, ))
                    .map(|result| result.affected_rows())
                    .unwrap();

                // drop used traded_at
                let drop_stmt =
                    format!(r#"DELETE FROM {s1} WHERE 1=1 LIMIT ?;"#, s1 = s1_table_name);
                tx.prep_exec(drop_stmt, (LIMIT, )).unwrap();

                tx.commit().unwrap();

                Ok(t)
            })
            .unwrap()
    }

    // write aggregated execution data
    fn step2(&self, data_table_name: &str, s1_table_name: &str, s2_table_name: &str) {
        // fetch all traded_at, and insert it
        while self.move_aggregated_data(s1_table_name, s2_table_name, data_table_name) != 0 {}
    }
}
