package mapper

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	user "user-service/internal/domain"
)

func TestMapper(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redis Mapper Suite")
}

var _ = Describe("MapDomainUserToCache", func() {
	It("maps the domain user into the redis cache model", func() {
		createdAt := time.Date(2026, time.April, 24, 8, 30, 0, 123000000, time.FixedZone("EEST", 3*60*60))
		updatedAt := time.Date(2026, time.April, 24, 9, 45, 0, 456000000, time.FixedZone("EEST", 3*60*60))

		cache := MapDomainUserToCache(&user.User{
			ID:        "user-1",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		})

		Expect(cache.ID).To(Equal("user-1"))
		Expect(cache.Username).To(Equal("alex"))
		Expect(cache.FirstName).To(Equal("Alex"))
		Expect(cache.LastName).To(Equal("Doe"))
		Expect(cache.CreatedAt).To(Equal(createdAt.UTC().Format("2006-01-02T15:04:05.999999999Z")))
		Expect(cache.UpdatedAt).To(Equal(updatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z")))
	})
})
