package main

import (
	"strings"
)

// ProviderGroup represents a healthcare provider group
type ProviderGroup struct {
	NPI []interface{} `json:"npi"` // Can be either string or number
	TIN struct {
		Type  string `json:"type,omitempty"`
		Value string `json:"value,omitempty"`
	} `json:"tin"`
}

// GetNPIStrings converts NPI values to strings
func (pg *ProviderGroup) GetNPIStrings() []string {
	var npiStrings []string
	for _, npi := range pg.NPI {
		npiStr := npiToString(npi)
		if npiStr != "" {
			npiStrings = append(npiStrings, npiStr)
		}
	}
	return npiStrings
}

// ProviderReference represents a reference to a provider group
type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
	Location        string          `json:"location,omitempty"` // URL for remote references
}

// RemoteReference represents a work item for fetching remote references
type RemoteReference struct {
	URL             string
	ProviderGroupID int
}

// NegotiatedPrice represents a negotiated price for a service
type NegotiatedPrice struct {
	NegotiatedRate        float64  `json:"negotiated_rate"`
	NegotiatedType        string   `json:"negotiated_type,omitempty"`
	BillingClass          string   `json:"billing_class,omitempty"`
	BillingCodeModifier   []string `json:"billing_code_modifier,omitempty"`
	ServiceCode           []string `json:"service_code,omitempty"`
	ExpirationDate        string   `json:"expiration_date,omitempty"`
	AdditionalInformation string   `json:"additional_information,omitempty"`
}

// IsValidNegotiatedType checks if the negotiated type is valid
func (p *NegotiatedPrice) IsValidNegotiatedType() bool {
	return p.NegotiatedType == "negotiated" || p.NegotiatedType == "fee schedule"
}

// NegotiatedRate represents a set of negotiated rates for providers
type NegotiatedRate struct {
	ProviderReferences []int             `json:"provider_references"`
	NegotiatedPrices   []NegotiatedPrice `json:"negotiated_prices"`
	ProviderGroups     []ProviderGroup   `json:"provider_groups,omitempty"`
}

// InNetworkItem represents an in-network service item
type InNetworkItem struct {
	NegotiationArrangement string            `json:"negotiation_arrangement"`
	Name                  string             `json:"name"`
	BillingCodeType       string             `json:"billing_code_type"`
	BillingCodeTypeVersion string            `json:"billing_code_type_version"`
	BillingCode           string             `json:"billing_code"`
	Description           string             `json:"description"`
	NegotiatedRates       []NegotiatedRate   `json:"negotiated_rates"`
}

// IsValidArrangement checks if the negotiation arrangement is valid
func (i *InNetworkItem) IsValidArrangement() bool {
	// Python version only processes 'ffs' arrangements
	return i.NegotiationArrangement == "" || i.NegotiationArrangement == "ffs"
}

// CleanString removes whitespace and returns empty string as is
func CleanString(s string) string {
	if s == "null" || s == "<nil>" {
		return ""
	}
	return strings.TrimSpace(s)
}

// CleanStringSlice removes empty strings from a slice after trimming
func CleanStringSlice(ss []string) []string {
	if ss == nil {
		return []string{}
	}
	cleaned := make([]string, 0, len(ss))
	for _, s := range ss {
		if cs := CleanString(s); cs != "" {
			cleaned = append(cleaned, cs)
		}
	}
	return cleaned
}
