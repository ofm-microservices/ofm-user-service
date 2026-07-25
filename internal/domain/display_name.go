package user

import "strings"

// DisplayName renders the public display name from the user profile's first
// and last names.
func DisplayName(firstName, lastName string) string {
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	switch {
	case firstName == "" && lastName == "":
		return ""
	case firstName == "":
		return lastName
	case lastName == "":
		return firstName
	default:
		return firstName + " " + lastName
	}
}
