pub mod app;
pub mod config;
pub mod connection;
pub mod consumer;
pub mod pipeline;
pub mod sender;
pub mod metrics;

// Re-export main application
pub use app::Application;