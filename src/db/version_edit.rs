use crate::db::internal_key::InternalKey;
use ahash::HashSet;

struct FileMetaData {
    refs: i32,
    allowed_seeks: i32,
    number: u64,
    file_size: u64,
    smallest: InternalKey,
    largest: InternalKey,
}

struct VersionEdit {
    comparator_: String,
    log_number_: u64,
    prev_log_number_: u64,
    next_file_number_: u64,
    last_sequence_: u64,
    has_comparator_: bool,
    has_log_number_: bool,
    has_prev_log_number_: bool,
    has_next_file_number_: bool,
    has_last_sequence_: bool,
    compact_pointers_: Vec<(i32, InternalKey)>,
    deleted_files: HashSet<(i32, u64)>,
    new_file: Vec<(i32, FileMetaData)>,
}


impl VersionEdit {
    pub fn new() -> Self {
      VersionEdit{
            comparator_: "".to_string(),
            log_number_: 0,
            prev_log_number_: 0,
            next_file_number_: 0,
            last_sequence_: 0,
            has_comparator_: false,
            has_log_number_: false,
            has_prev_log_number_: false,
            has_next_file_number_: false,
            has_last_sequence_: false,
            compact_pointers_: vec![],
            deleted_files: HashSet::default(),
            new_file: vec![],
        }
    }

    pub fn clear(&mut self) {
        self.comparator_.clear();
        self.log_number_ = 0;
        self.prev_log_number_ = 0;
        self.last_sequence_ = 0;
        self.next_file_number_ = 0;
        self.has_comparator_ = false;
        self.has_log_number_ = false;
        self.has_prev_log_number_ = false;
        self.has_next_file_number_ = false;
        self.has_last_sequence_ = false;
        self.compact_pointers_.clear();
        self.deleted_files.clear();
        self.new_file.clear();
    }


    pub fn set_comparator_name(&mut self, name:String) {
       self.has_comparator_ = true;
        self.comparator_ = name;
    }

    pub fn set_log_number_(&mut self, log_number_: u64) {
        self.has_log_number_ = true;
        self.log_number_ = log_number_;
    }

    pub fn set_prev_log_number_(&mut self, prev_log_number_: u64) {
        self.has_prev_log_number_= true;
        self.prev_log_number_ = prev_log_number_;
    }

    pub fn set_next_file_number_(&mut self, next_file_number_: u64) {
        self.has_next_file_number_ = true;
        self.next_file_number_ = next_file_number_;
    }

    pub fn set_last_sequence_(&mut self, last_sequence_: u64) {
        self.has_last_sequence_ = true;
        self.last_sequence_ = last_sequence_;
    }

    pub fn set_compact_pointers_(&mut self,level:i32,  key:InternalKey) {
        self.compact_pointers_.push((level, key))
    }

    pub fn add_file(&mut self, level:i32, file:u64, file_size:u64, smallest:InternalKey, largest:InternalKey) {
        let f = FileMetaData{
            refs: 0,
            allowed_seeks: 0,
            number: file,
            file_size,
            smallest,
            largest,
        };
        self.new_file.push((level, f))
    }

    pub fn remove_file(&mut self, level:i32, file:u64) {
        self.deleted_files.insert((level, file));
    }
}