pub mod accumulate;
pub mod codec;
pub mod decoder;
pub mod event;
pub mod messages_emit;
#[cfg(feature = "std")]
pub mod pipeline;
mod processor;
pub mod responses_emit;
mod terminal;
mod transformer;
pub mod translate;

#[cfg(feature = "std")]
pub use accumulate::{accumulate, accumulate_bytes};
pub use accumulate::{accumulate_slice, SseAccumulator};
pub use codec::{encode_done, encode_sse};
pub use decoder::{DecodeError, Decoder, Record};
pub use event::SseEvent;
#[cfg(feature = "std")]
pub use pipeline::{observer_channel, ObserverMsg, ObserverReceiver, ObserverSender, SsePipeline, TapKinds};
pub use processor::{StreamOutcome, StreamProcessor};
pub use terminal::TerminalState;
pub use transformer::EventTransformer;
pub use translate::EndpointTranslator;
