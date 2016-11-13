package common

const SEPARATOR = "_"

func GenerateId(customerno string, platform string, tenantId string) string {
	return customerno + SEPARATOR + platform + SEPARATOR + tenantId;
}

