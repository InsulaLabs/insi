package provider

type NewProviderRequest struct {
	DisplayName string `json:"display_name"`
	Provider    string `json:"provider"`
	APIKey      string `json:"api_key"`
	BaseURL     string `json:"base_url"`
}

type DeleteProviderRequest struct {
	UUID string `json:"uuid"`
}

type UpdateProviderDisplayNameRequest struct {
	UUID        string `json:"uuid"`
	DisplayName string `json:"display_name"`
}

type UpdateProviderAPIKeyRequest struct {
	UUID   string `json:"uuid"`
	APIKey string `json:"api_key"`
}

type UpdateProviderBaseURLRequest struct {
	UUID    string `json:"uuid"`
	BaseURL string `json:"base_url"`
}
