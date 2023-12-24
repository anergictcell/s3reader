#![allow(dead_code)]

use crate::external_types;


struct FileType {}
impl FileType {
    pub fn is_dir(&self) -> bool {
        // TODO
        false
    }

    pub fn is_file(&self) -> bool {
        // TODO
        true
    }

    pub fn is_symlink(&self) -> bool {
        // TODO
        false
    }
}


struct Permissions {}
impl Permissions {
    pub fn readonly(&self) -> bool {
        true
    }
}

struct Metadata {
    s3_head: external_types::HeadObjectOutput
}

impl Metadata {
    pub fn file_type(&self) -> FileType {
        FileType {}
    }

    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    pub fn len(&self) -> u64 {
        // TODO
        0
    }

    pub fn permissions(&self) -> Permissions {
        Permissions {}
    }

    pub fn modified(&self) -> Option<&external_types::DateTime> {
        self.s3_head.last_modified()
    }
}