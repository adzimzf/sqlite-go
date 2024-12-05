package db

// DatabaseHeader represents the SQLite database header structure
type DatabaseHeader struct {
	HeaderString        [100]byte
	PageSize            uint16
	FileFormatWrite     byte
	FileFormatRead      byte
	Reserved1           byte
	MaxEmbeddedPayload  byte
	MinEmbeddedPayload  byte
	LeafPayloadFraction byte
	FileChangeCounter   uint32
	SizeOfPageCache     uint32
	SchemaCookie        uint32
	SchemaFormat        uint32
	DefaultPageCache    byte
	IncrementalVacuum   byte
	ApplicationID       uint32
	VersionValidFor     uint32
	VersionUsed         uint32
}
