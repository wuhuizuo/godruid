package godruid

type ExtractionFn interface{}

type RegisteredLookupExtractionFn struct {
	Type   string `json:"type"`
	Lookup string `json:"lookup`
}

type TimeExtractionFn struct {
	Type     string `json:"type"`
	Format   string `json:"format"`
	TimeZone string `json:"timeZone"`
	Locale   string `json:"locale"`
}
