package mapper

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"user-service/internal/infra/write/yugabyte/model"
)

func TestMapper(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Yugabyte Mapper Suite")
}

var _ = Describe("MapUserRowToDomain", func() {
	It("maps the yugabyte row into the domain user", func() {
		createdAt := time.Date(2026, time.April, 24, 8, 30, 0, 0, time.UTC)
		updatedAt := time.Date(2026, time.April, 24, 9, 45, 0, 0, time.UTC)

		user := MapUserRowToDomain(model.UserRow{
			ID:        "user-1",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		})

		Expect(user).NotTo(BeNil())
		Expect(user.ID).To(Equal("user-1"))
		Expect(user.Username).To(Equal("alex"))
		Expect(user.FirstName).To(Equal("Alex"))
		Expect(user.LastName).To(Equal("Doe"))
		Expect(user.CreatedAt).To(Equal(createdAt))
		Expect(user.UpdatedAt).To(Equal(updatedAt))
	})
})
