package common

type BatchUpdateBean struct {
	Customers []string 	`json:"customers"`
	Platform string    	`json:"platform"`
	TenantId string   	`json:"tenantId"`
	PropertyId string   `json:"propertyId"`
	Value string       	`json:"value"`
}