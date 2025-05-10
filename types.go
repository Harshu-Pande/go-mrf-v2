package main

// ProviderGroup represents a healthcare provider group
type ProviderGroup struct {
	NPI interface{} `json:"npi,string,omitempty"`
	TIN struct {
		Type  string `json:"type,omitempty"`
		Value string `json:"value,omitempty"`
	} `json:"tin"`
}

// ProviderReference represents a reference to a provider group
type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
}

// NegotiatedPrice represents a negotiated price for a service
type NegotiatedPrice struct {
	NegotiatedRate  float64 `json:"negotiated_rate"`
	NegotiatedType  string  `json:"negotiated_type,omitempty"`
	BillingClass    string  `json:"billing_class,omitempty"`
}

// NegotiatedRate represents a set of negotiated rates for providers
type NegotiatedRate struct {
	ProviderReferences []int             `json:"provider_references"`
	NegotiatedPrices  []NegotiatedPrice `json:"negotiated_prices"`
}

// InNetworkItem represents an in-network service item
type InNetworkItem struct {
	BillingCode            string           `json:"billing_code"`
	BillingCodeType        string           `json:"billing_code_type,omitempty"`
	NegotiationArrangement string           `json:"negotiation_arrangement,omitempty"`
	NegotiatedRates       []NegotiatedRate `json:"negotiated_rates"`
} 