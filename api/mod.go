package api

import (
	"context"
	"encoding/hex"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/render"
	"github.com/perlin-network/noise"
	"github.com/perlin-network/wavelet"
	"github.com/perlin-network/wavelet/log"
	"github.com/perlin-network/wavelet/node"
	"github.com/pkg/errors"
	"net/http"
	"strconv"
	"time"
)

type hub struct {
	node   *noise.Node
	ledger *wavelet.Ledger

	registry *sessionRegistry
}

func StartHTTP(n *noise.Node, port int) {
	h := &hub{node: n, ledger: node.Ledger(n), registry: newSessionRegistry()}

	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	//r.Use(middleware.Logger)
	cors := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	})
	r.Use(cors.Handler)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Handle("/debug/vars", http.DefaultServeMux)

	r.Route("/session", func(r chi.Router) {
		r.Post("/init", h.initSession)
	})

	r.Route("/tx", func(r chi.Router) {
		r.With(h.authenticated).Get("/", h.listTransactions)
		r.Get("/{id:^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$}", h.getTransaction)
		r.With(h.authenticated).Post("/send", h.sendTransaction)
	})

	r.Route("/ledger", func(r chi.Router) {
		r.Get("/state", h.ledgerStatus)
	})

	log.Info().Msgf("Started HTTP API server on port %d.", port)

	http.ListenAndServe(":"+strconv.Itoa(port), r)
}

func (h *hub) initSession(w http.ResponseWriter, r *http.Request) {
	req := new(SessionInitRequest)

	if err := render.Bind(r, req); err != nil {
		render.Render(w, r, ErrBadRequest(err))
		return
	}

	session, err := h.registry.newSession()
	if err != nil {
		render.Render(w, r, ErrBadRequest(errors.Wrap(err, "failed to create session")))
	}

	render.Render(w, r, &SessionInitResponse{Token: session.id})
}

func (h *hub) sendTransaction(w http.ResponseWriter, r *http.Request) {
	req := new(SendTransactionRequest)

	if err := render.Bind(r, req); err != nil {
		render.Render(w, r, ErrBadRequest(err))
		return
	}

	tx := &wavelet.Transaction{
		Creator:          req.creator,
		CreatorSignature: req.signature,

		Tag:     req.Tag,
		Payload: req.Payload,
	}

	if err := h.ledger.AttachSenderToTransaction(h.node.Keys, tx); err != nil {
		render.Render(w, r, ErrInternal(errors.Wrap(err, "failed to attach sender to transaction")))
		return
	}

	if err := node.BroadcastTransaction(h.node, tx); err != nil {
		render.Render(w, r, ErrInternal(errors.Wrap(err, "failed to broadcast transaction")))
		return
	}

	render.Render(w, r, &SendTransactionResponse{ledger: h.ledger, tx: tx})
}

func (h *hub) ledgerStatus(w http.ResponseWriter, r *http.Request) {
	render.Render(w, r, &LedgerStatusResponse{node: h.node, ledger: h.ledger})
}

func (h *hub) listTransactions(w http.ResponseWriter, r *http.Request) {
	var offset, limit uint64
	var err error

	if raw := chi.URLParam(r, "offset"); len(raw) > 0 {
		offset, err = strconv.ParseUint(raw, 10, 64)

		if err != nil {
			render.Render(w, r, ErrBadRequest(errors.Wrap(err, "could not parse offset")))
			return
		}
	}

	if raw := chi.URLParam(r, "limit"); len(raw) > 0 {
		limit, err = strconv.ParseUint(raw, 10, 64)

		if err != nil {
			render.Render(w, r, ErrBadRequest(errors.Wrap(err, "could not parse limit")))
		}
	}

	var transactions []render.Renderer

	for _, tx := range h.ledger.Transactions(offset, limit) {
		transactions = append(transactions, &Transaction{tx: tx})
	}

	render.RenderList(w, r, transactions)
}

func (h *hub) getTransaction(w http.ResponseWriter, r *http.Request) {
	param := chi.URLParam(r, "id")

	slice, err := hex.DecodeString(param)
	if err != nil {
		render.Render(w, r, ErrBadRequest(errors.Wrap(err, "transaction ID must be presented as valid hex")))
		return
	}

	if len(slice) != wavelet.TransactionIDSize {
		render.Render(w, r, ErrBadRequest(errors.Errorf("transaction ID must be %d bytes long", wavelet.TransactionIDSize)))
		return
	}

	var id [wavelet.TransactionIDSize]byte
	copy(id[:], slice)

	tx := h.ledger.FindTransaction(id)

	if tx == nil {
		render.Render(w, r, ErrBadRequest(errors.Errorf("could not find transaction with ID ", param)))
		return
	}

	render.Render(w, r, &Transaction{tx: tx})
}

func (h *hub) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(HeaderSessionToken)

		if len(token) == 0 {
			render.Render(w, r, ErrBadRequest(errors.Errorf("missing HTTP header %q", HeaderSessionToken)))
			return
		}

		session, exists := h.registry.getSession(token)
		if !exists {
			render.Render(w, r, ErrBadRequest(errors.Errorf("could not find session %q", token)))
			return
		}

		ctx := context.WithValue(r.Context(), "session", session)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
