package types

type Order struct {
	CustomerName string `json:"customer_name" validate:"required"`
	CoffeeType   string `json:"coffee_type" validate:"required"`
}
