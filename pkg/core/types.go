package core

import (
	"encoding/json"

	"github.com/nikolaydubina/fpdecimal"
)

// TradeOrder structure
type TradeOrder struct {
	OrderID  string
	Role     Role
	Price    fpdecimal.Decimal
	IsQuote  bool
	Quantity fpdecimal.Decimal
}

// MarshalJSON implements Marshaler interface
func (t *TradeOrder) MarshalJSON() ([]byte, error) {
	customStruct := struct {
		OrderID  string `json:"orderID"`
		Role     Role   `json:"role"`
		IsQuote  bool   `json:"isQuote"`
		Price    string `json:"price"`
		Quantity string `json:"quantity"`
	}{
		OrderID:  t.OrderID,
		Role:     t.Role,
		IsQuote:  t.IsQuote,
		Price:    t.Price.String(),
		Quantity: t.Quantity.String(),
	}
	return json.Marshal(customStruct)
}

// Done structure represents the results of order processing
type Done struct {
	Order     *Order
	Trades    []*TradeOrder
	Canceled  []string
	Activated []string
	Stored    bool
	Quantity  fpdecimal.Decimal
	Left      fpdecimal.Decimal
	Processed fpdecimal.Decimal
}

// Helper functions for Done
func newDone(order *Order) *Done {
	return &Done{
		Order:     order,
		Trades:    make([]*TradeOrder, 0),
		Canceled:  make([]string, 0),
		Activated: make([]string, 0),
		Quantity:  order.OriginalQty(),
		Left:      fpdecimal.Zero,
		Processed: fpdecimal.Zero,
	}
}

// GetTradeOrder returns TradeOrder by id
func (d *Done) GetTradeOrder(id string) *TradeOrder {
	for _, t := range d.Trades {
		if t.OrderID == id {
			return t
		}
	}
	return nil
}

// appendOrder adds a trade to the Done object
func (d *Done) appendOrder(order *Order, quantity, price fpdecimal.Decimal) {
	if len(d.Trades) == 0 {
		d.Trades = append(d.Trades, newTradeOrder(d.Order, fpdecimal.Zero, d.Order.Price()))
	}

	d.Trades = append(d.Trades, newTradeOrder(order, quantity, price))
}

// tradesToSlice converts trades to a slice
func (d *Done) tradesToSlice() []TradeOrder {
	slice := make([]TradeOrder, 0, len(d.Trades))
	for _, v := range d.Trades {
		slice = append(slice, *v)
	}
	return slice
}

// appendCanceled adds a canceled order ID to the Done object
func (d *Done) appendCanceled(order *Order) {
	d.Canceled = append(d.Canceled, order.ID())
}

// appendActivated adds an activated order ID to the Done object
func (d *Done) appendActivated(order *Order) {
	d.Activated = append(d.Activated, order.ID())
}

// setLeftQuantity sets the left and processed quantities
func (d *Done) setLeftQuantity(quantity *fpdecimal.Decimal) {
	if quantity == nil {
		return
	}

	d.Left = *quantity
	d.Processed = d.Quantity.Sub(d.Left)

	if len(d.Trades) != 0 {
		d.Trades[0].Quantity = d.Processed
	}
}

// Helper function to create trade orders
func newTradeOrder(order *Order, quantity, price fpdecimal.Decimal) *TradeOrder {
	if order == nil {
		return nil
	}
	return &TradeOrder{
		OrderID:  order.ID(),
		Role:     order.Role(),
		Price:    price,
		IsQuote:  order.IsQuote(),
		Quantity: quantity,
	}
}

// MarshalJSON implements json.Marshaler interface for Done
func (d *Done) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Order     *TradeOrder  `json:"order"`
		Trades    []TradeOrder `json:"trades"`
		Canceled  []string     `json:"canceled"`
		Activated []string     `json:"activated"`
		Left      string       `json:"left"`
		Processed string       `json:"processed"`
		Stored    bool         `json:"stored"`
	}{
		Order:     d.Order.ToSimple(),
		Trades:    d.tradesToSlice(),
		Canceled:  d.Canceled,
		Activated: d.Activated,
		Left:      d.Left.String(),
		Processed: d.Processed.String(),
		Stored:    d.Stored,
	})
}
