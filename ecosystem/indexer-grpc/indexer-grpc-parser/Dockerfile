FROM aptoslabs/indexer-grpc:indexer-grpc-preview@sha256:875213213af447e18a64261779dd80a8a94b51373ef89a56cf3efaf127b2087a
ADD parser.yaml /configs/parser.yaml

WORKDIR /usr/local/bin

CMD ["aptos-indexer-grpc-parser", "-c", "/configs/parser.yaml"]