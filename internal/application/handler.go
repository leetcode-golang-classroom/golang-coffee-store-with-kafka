package application

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-playground/validator"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/broker"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/types"
	"github.com/leetcode-golang-classroom/golang-coffee-store-with-kafka/internal/util"
)

type Handler struct {
	broker *broker.Broker
}

func NewHandler(broker *broker.Broker) *Handler {
	return &Handler{broker}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /order", h.PlaceOrder)
}
func (h *Handler) PlaceOrder(w http.ResponseWriter, r *http.Request) {
	var orderRequest types.Order
	// parse data into order
	if err := util.ParseJSON(r, &orderRequest); err != nil {
		util.WriteError(w, http.StatusBadRequest, err)
		return
	}
	// validate
	if err := util.Validdate.Struct(orderRequest); err != nil {
		var valErrs validator.ValidationErrors
		if errors.As(err, &valErrs) {
			util.WriteError(w, http.StatusBadRequest, fmt.Errorf("invalid payload:%v", valErrs))
		}
		return
	}
	// Convert body into bytes
	data, err := json.Marshal(orderRequest)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Errorf("failed to marshal order :%w", err))
		return
	}
	// Send the bytes to kafka
	err = h.broker.PushOrderToQueue(r.Context(), "coffee_orders", data)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Errorf("failed to send kafka :%w", err))
		return
	}
	// Response bakc to user
	response := map[string]interface{}{
		"success": true,
		"msg":     fmt.Sprintf("Order for %s placed successfully", orderRequest.CustomerName),
	}
	util.FailOnError(util.WriteJSON(w, http.StatusCreated, response), "failed on response json")
}
