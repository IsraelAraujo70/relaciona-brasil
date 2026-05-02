//! Acesso ao dump da Receita Federal: descoberta, download, descompressão,
//! decodificação e parsing dos CSVs. Tudo streaming, sem buffering em massa.

pub mod decode;
pub mod download;
pub mod nextcloud;
pub mod parse;
pub mod scan;
pub mod unzip;
pub mod vintage;
