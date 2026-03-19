# Raw Envelope v1

`RawEnvelope` is the first stable internal shape emitted after source capture.

It must preserve source-native detail strongly enough to support:

- debugging
- replay
- re-normalization
- sink redaction and export

The runtime may enrich a raw envelope with parse hints, but it must not replace the source-native payload with product-specific semantics.

