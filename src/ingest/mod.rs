//! Pipeline de ingestão do dump da Receita Federal.

pub mod checkpoint;
pub mod decode;
pub mod download;
pub mod load;
pub mod parse;
pub mod pgcopy;
pub mod pipeline;
pub mod source;
pub mod unzip;
