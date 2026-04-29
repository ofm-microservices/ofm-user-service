package grpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	userv1 "github.com/ofm-microseervices/ofm-common/proto/user/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"user-service/config"
)

func TestGRPC(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "GRPC Suite")
}

var _ = Describe("Server", func() {
	var (
		ctrl   *gomock.Controller
		svc    *MockUserService
		logger logging.Logger
		cfg    config.GRPCConfig
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		svc = NewMockUserService(ctrl)
		cfg = config.GRPCConfig{Host: "127.0.0.1", Port: 50051}

		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("NewServer", func() {
		It("validates nil collaborators", func() {
			server, err := NewServer(nil, cfg, logger)
			Expect(server).To(BeNil())
			Expect(err).To(MatchError(ErrNilUserService))

			server, err = NewServer(svc, cfg, nil)
			Expect(server).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})

		It("constructs a grpc server", func() {
			server, err := NewServer(svc, cfg, logger)

			Expect(err).NotTo(HaveOccurred())
			Expect(server).NotTo(BeNil())
		})
	})

	Describe("ExistsByUsername", func() {
		It("returns the application result", func() {
			srv, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			svc.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(true, nil)

			resp, err := srv.(*server).ExistsByUsername(context.Background(), &userv1.ExistsByUsernameRequest{
				Username: "alex",
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Exists).To(BeTrue())
		})

		It("maps service failures to internal grpc errors", func() {
			srv, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			svc.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(false, errors.New("boom"))

			resp, err := srv.(*server).ExistsByUsername(context.Background(), &userv1.ExistsByUsernameRequest{
				Username: "alex",
			})

			Expect(resp).To(BeNil())
			Expect(status.Code(err)).To(Equal(codes.Internal))
			Expect(status.Convert(err).Message()).To(Equal("internal server error"))
		})
	})

	Describe("Shutdown", func() {
		It("returns nil when nothing was started", func() {
			server, err := NewServer(svc, cfg, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(server.Shutdown(context.Background())).To(Succeed())
		})
	})

	Describe("Start", func() {
		It("returns a listen error when the address is already in use", func() {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			Expect(err).NotTo(HaveOccurred())
			defer lis.Close()

			addr := lis.Addr().(*net.TCPAddr)
			srv, err := NewServer(svc, config.GRPCConfig{Host: "127.0.0.1", Port: addr.Port}, logger)
			Expect(err).NotTo(HaveOccurred())

			Expect(srv.Start()).To(HaveOccurred())
		})

		It("serves requests and shuts down cleanly", func() {
			srv, err := NewServer(svc, config.GRPCConfig{Host: "127.0.0.1", Port: 19093}, logger)
			Expect(err).NotTo(HaveOccurred())

			serverErr := make(chan error, 1)
			go func() {
				serverErr <- srv.Start()
			}()

			connCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := ggrpc.DialContext(connCtx, "127.0.0.1:19093",
				ggrpc.WithTransportCredentials(insecure.NewCredentials()),
				ggrpc.WithBlock(),
			)
			Expect(err).NotTo(HaveOccurred())
			defer conn.Close()

			client := userv1.NewUserQueryServiceClient(conn)
			svc.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(true, nil)

			resp, err := client.ExistsByUsername(context.Background(), &userv1.ExistsByUsernameRequest{Username: "alex"})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Exists).To(BeTrue())

			Expect(srv.Shutdown(context.Background())).To(SatisfyAny(
				Succeed(),
				MatchError(ContainSubstring("use of closed network connection")),
			))
			Eventually(serverErr).Should(Receive(SatisfyAny(
				BeNil(),
				MatchError(ContainSubstring("use of closed network connection")),
			)))
		})
	})
})
