package repository

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	user "user-service/internal/domain"
)

var _ = Describe("repository unit", func() {
	It("validates constructor inputs and nil users", func() {
		repoAny, err := New(nil)
		Expect(repoAny).To(BeNil())
		Expect(err).To(MatchError(ErrNilRedisClient))

		client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		defer func() { Expect(client.Close()).To(Succeed()) }()

		repoAny, err = New(client)
		Expect(err).NotTo(HaveOccurred())
		Expect(repoAny.Upsert(context.Background(), nil)).To(MatchError(ErrNilUser))
		Expect(UserCacheKey("user-1")).To(Equal("user:user-1"))
	})

	It("wraps set and delete failures", func() {
		client := redis.NewClient(&redis.Options{
			Addr:         "127.0.0.1:1",
			DialTimeout:  time.Millisecond,
			ReadTimeout:  time.Millisecond,
			WriteTimeout: time.Millisecond,
			MaxRetries:   0,
		})
		defer func() { Expect(client.Close()).To(Succeed()) }()

		repoAny, err := New(client)
		Expect(err).NotTo(HaveOccurred())

		err = repoAny.Upsert(context.Background(), &user.User{ID: "user-1", Username: "alex"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("set user cache"))

		err = repoAny.DeleteByID(context.Background(), "user-1")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("delete user cache"))
	})
})
