package main

import (
	"fmt"

	"seeders/shared"
)

func BuildEventTypeUUID(eventTypeID uint32, created string) string {
	createdString := shared.SafeConvertToDateTimeString(created)
	UUIDSeed := fmt.Sprintf("%d-%s", eventTypeID, createdString)
	UUID := shared.GenerateUUIDFromString(UUIDSeed)
	return UUID
}

func main() {
	// add the UUID input here
	// log.Println(BuildEventTypeUUID())
}