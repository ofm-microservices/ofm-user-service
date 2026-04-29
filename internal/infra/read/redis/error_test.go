package repository

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("error wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapMarshalUserCacheError(cause)).To(MatchError(ContainSubstring("marshal user cache")))
		Expect(WrapSetUserCacheError("user:1", cause)).To(MatchError(ContainSubstring(`set user cache by key "user:1"`)))
		Expect(WrapDeleteUserCacheError("user:1", cause)).To(MatchError(ContainSubstring(`delete user cache by key "user:1"`)))
	})
})
