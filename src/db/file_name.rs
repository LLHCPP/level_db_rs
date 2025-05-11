
fn make_file_name(db_name: &String, number: u64, suffix: &str) -> String {
    let result = format!("{db_name}/{number:06}.{suffix}");
    result
}

pub fn table_file_name(db_name: &String, number: u64) ->String {
    debug_assert!(number >0);
    return make_file_name(db_name, number, "ldb")
}

pub fn sst_table_file_name(db_name: &String, number: u64) ->String {
    debug_assert!(number >0);
    return make_file_name(db_name, number, "sst")
}

mod test{
    use crate::db::file_name::{make_file_name, table_file_name};
    use std::env;
    #[test]
    fn test_table_file_name(){
        let db_name = "test";
        let number = 1001;
        let result = table_file_name(&db_name.to_string(), number);
        assert_eq!(result, "test/001001.ldb");
    }
}