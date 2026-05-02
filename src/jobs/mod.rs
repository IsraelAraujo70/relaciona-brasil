//! Fila de jobs assíncronos. API enfileira, worker consome, resultado fica
//! disponível por polling em `/v1/jobs/{id}` ou via webhook opcional.

pub mod queue;
pub mod webhook;
