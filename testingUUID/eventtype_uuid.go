package main

import (
	// "fmt"
	"log"
	"seeders/shared"
)

// func BuildEventTypeUUID(eventTypeID uint32, created string) string {
func BuildEventTypeUUID() string {
	// createdString := shared.SafeConvertToDateTimeString(created)
	// UUIDSeed := fmt.Sprintf("%d-%s", eventTypeID, createdString)
	stringInput := "IN-2013-07-15 20:56:10"
	UUID := shared.GenerateUUIDFromString(stringInput)
	return UUID
}

func main() {
	// add the UUID input here
	log.Println(BuildEventTypeUUID())
}