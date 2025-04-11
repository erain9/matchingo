package core

import (
	"fmt"
	"strings"

	"github.com/erain9/matchingo/pkg/db/queue"
	"github.com/erain9/matchingo/pkg/messaging"
	"github.com/nikolaydubina/fpdecimal"
)

// OrderBook implements standard matching algorithm
type OrderBook struct {
	backend OrderBookBackend
}

// NewOrderBook creates Orderbook object with a backend
func NewOrderBook(backend OrderBookBackend) *OrderBook {
	return &OrderBook{
		backend: backend,
	}
}

// GetOrder returns Order by id
func (ob *OrderBook) GetOrder(orderID string) *Order {
	return ob.backend.GetOrder(orderID)
}

// CancelOrder removes Order with given ID from the Order book or the Stop book
func (ob *OrderBook) CancelOrder(orderID string) *Order {
	order := ob.GetOrder(orderID)
	if order == nil {
		return nil
	}

	order.Cancel()

	if order.IsStopOrder() {
		ob.backend.RemoveFromStopBook(order)
		ob.backend.DeleteOrder(order.ID())
	} else {
		ob.deleteOrder(order)
	}

	return order
}

// Process public method
func (ob *OrderBook) Process(order *Order) (done *Done, err error) {
	if order.IsMarketOrder() {
		return ob.processMarketOrder(order)
	}

	if order.IsLimitOrder() {
		return ob.processLimitOrder(order)
	}

	if order.IsStopOrder() {
		return ob.processStopOrder(order)
	}

	panic("unrecognized order type")
}

// CalculateMarketPrice returns total market Price for requested quantity
func (ob *OrderBook) CalculateMarketPrice(side Side, quantity fpdecimal.Decimal) (price fpdecimal.Decimal, err error) {
	price = fpdecimal.Zero
	remaining := quantity

	// For buy orders, check sell side (asks)
	// For sell orders, check buy side (bids)
	if side == Buy {
		asks := ob.backend.GetAsks()
		if askSide, isOrderSide := asks.(interface {
			Prices() []fpdecimal.Decimal
			Orders(price fpdecimal.Decimal) []*Order
		}); isOrderSide {
			prices := askSide.Prices()
			for _, p := range prices {
				orders := askSide.Orders(p)
				for _, order := range orders {
					orderQty := order.Quantity()
					if remaining.LessThanOrEqual(orderQty) {
						// This order can fill the entire remaining quantity
						price = price.Add(p.Mul(remaining))
						remaining = fpdecimal.Zero
						break
					} else {
						// This order can only partially fill the remaining quantity
						price = price.Add(p.Mul(orderQty))
						remaining = remaining.Sub(orderQty)
					}
				}
				if remaining.Equal(fpdecimal.Zero) {
					break
				}
			}
		}
	} else {
		bids := ob.backend.GetBids()
		if bidSide, isOrderSide := bids.(interface {
			Prices() []fpdecimal.Decimal
			Orders(price fpdecimal.Decimal) []*Order
		}); isOrderSide {
			prices := bidSide.Prices()
			for _, p := range prices {
				orders := bidSide.Orders(p)
				for _, order := range orders {
					orderQty := order.Quantity()
					if remaining.LessThanOrEqual(orderQty) {
						// This order can fill the entire remaining quantity
						price = price.Add(p.Mul(remaining))
						remaining = fpdecimal.Zero
						break
					} else {
						// This order can only partially fill the remaining quantity
						price = price.Add(p.Mul(orderQty))
						remaining = remaining.Sub(orderQty)
					}
				}
				if remaining.Equal(fpdecimal.Zero) {
					break
				}
			}
		}
	}

	if !remaining.Equal(fpdecimal.Zero) {
		return fpdecimal.Zero, ErrInsufficientQuantity
	}

	return price, nil
}

// private methods

func (ob *OrderBook) deleteOrder(order *Order) {
	ob.backend.DeleteOrder(order.ID())

	if order.Side() == Buy {
		ob.backend.RemoveFromSide(Buy, order)
	}

	if order.Side() == Sell {
		ob.backend.RemoveFromSide(Sell, order)
	}
}

func (ob *OrderBook) processMarketOrder(marketOrder *Order) (done *Done, err error) {
	quantity := marketOrder.Quantity()

	if quantity.LessThanOrEqual(fpdecimal.Zero) {
		return nil, ErrInvalidQuantity
	}

	// Store the order first
	err = ob.backend.StoreOrder(marketOrder)
	if err != nil {
		return nil, err
	}

	done = newDone(marketOrder)

	// Set market order as taker
	marketOrder.SetTaker()

	// Get the remaining quantity to process
	remainingQty := quantity

	// Process against the opposite side of the book
	oppositeOrders := ob.getOppositeOrders(marketOrder.Side())
	if ordersInterface, isOrderSideInterface := oppositeOrders.(interface {
		Prices() []fpdecimal.Decimal
		Orders(price fpdecimal.Decimal) []*Order
	}); isOrderSideInterface {
		prices := ordersInterface.Prices()

		// Iterate through price levels
		for _, price := range prices {
			if remainingQty.Equal(fpdecimal.Zero) {
				break
			}

			// Get all orders at this price level
			orders := ordersInterface.Orders(price)

			// Process each order at this price level
			for _, order := range orders {
				if remainingQty.Equal(fpdecimal.Zero) {
					break
				}

				// Determine the execution quantity
				executionQty := min(remainingQty, order.Quantity())

				// Execute the trade
				remainingQty = remainingQty.Sub(executionQty)

				// Update the matched order's quantity
				order.DecreaseQuantity(executionQty)

				// Record the trade in the done object
				done.appendOrder(order, executionQty, price)

				// Update the order in the backend
				err = ob.backend.UpdateOrder(order)
				if err != nil {
					return nil, err
				}

				// If order is fully executed, remove it from the book
				if order.Quantity().Equal(fpdecimal.Zero) {
					ob.deleteOrder(order)
				}
			}
		}
	}

	// Update the processed and left quantities in the done object
	done.setLeftQuantity(&remainingQty)

	// Delete the market order
	ob.backend.DeleteOrder(marketOrder.ID())

	return done, nil
}

func (ob *OrderBook) processLimitOrder(limitOrder *Order) (done *Done, err error) {
	quantity := limitOrder.Quantity()

	// Check if order exists
	order := ob.GetOrder(limitOrder.ID())
	if order != nil {
		return nil, ErrOrderExists
	}

	// Store the order first
	err = ob.backend.StoreOrder(limitOrder)
	if err != nil {
		return nil, err
	}

	done = newDone(limitOrder)

	// Check for OCO (One Cancels Other)
	if ob.checkOCO(limitOrder, done) {
		return done, nil
	}

	// Set limit order as taker initially
	limitOrder.SetTaker()

	// Get remaining quantity to process
	remainingQty := quantity
	orderPrice := limitOrder.Price()
	orderSide := limitOrder.Side()

	// Get the opposite side of the book
	oppositeOrders := ob.getOppositeOrders(orderSide)
	if ordersInterface, isOrderSideInterface := oppositeOrders.(interface {
		Prices() []fpdecimal.Decimal
		Orders(price fpdecimal.Decimal) []*Order
	}); isOrderSideInterface {
		prices := ordersInterface.Prices()

		// Iterate through price levels
		for _, price := range prices {
			// Check if price conditions are met for matching
			if !isPriceMatching(orderSide, orderPrice, price) {
				break
			}

			if remainingQty.Equal(fpdecimal.Zero) {
				break
			}

			// Get all orders at this price level
			orders := ordersInterface.Orders(price)

			// Process each order at this price level
			for _, order := range orders {
				if remainingQty.Equal(fpdecimal.Zero) {
					break
				}

				// Determine the execution quantity
				executionQty := min(remainingQty, order.Quantity())

				// Execute the trade
				remainingQty = remainingQty.Sub(executionQty)

				// Update the matched order's quantity
				order.DecreaseQuantity(executionQty)

				// Record the trade in the done object
				done.appendOrder(order, executionQty, price)

				// Update the order in the backend
				err = ob.backend.UpdateOrder(order)
				if err != nil {
					return nil, err
				}

				// If order is fully executed, remove it from the book
				if order.Quantity().Equal(fpdecimal.Zero) {
					ob.deleteOrder(order)
				}
			}
		}
	}

	// If there's remaining quantity, add the order to the book
	if !remainingQty.Equal(fpdecimal.Zero) {
		// Update order quantity to the remaining amount
		limitOrder.SetQuantity(remainingQty)

		// Set as maker since it's going in the book
		limitOrder.SetMaker()

		// Add to the appropriate side of the book
		ob.backend.AppendToSide(orderSide, limitOrder)

		// Mark as stored in the done object
		done.Stored = true
	} else {
		// Fully executed, remove from the system
		ob.backend.DeleteOrder(limitOrder.ID())
	}

	// Update the processed and left quantities in the done object
	done.setLeftQuantity(&remainingQty)

	// Correct the conversion logic for Done to DoneMessage
	queueSender := &queue.QueueMessageSender{}
	doneMessage := &messaging.DoneMessage{
		OrderID:      done.Order.ID(),
		ExecutedQty:  done.Processed.String(),
		RemainingQty: done.Left.String(),
		Trades:       convertTrades(done.tradesToSlice()),
		Canceled:     done.Canceled,
		Activated:    done.Activated,
		Stored:       done.Stored,
		Quantity:     done.Quantity.String(),
		Processed:    done.Processed.String(),
		Left:         done.Left.String(),
	}

	// Send the DoneMessage to Kafka. This won't block the call
	// if it returns error.
	if err := queueSender.SendDoneMessage(doneMessage); err != nil {
		fmt.Println(err) // TODO: Use logger in orderbook function.
	}

	return done, nil
}

func (ob *OrderBook) processStopOrder(stopOrder *Order) (done *Done, err error) {
	// Check if order exists
	order := ob.GetOrder(stopOrder.ID())
	if order != nil {
		return nil, ErrOrderExists
	}

	// Store the order first
	err = ob.backend.StoreOrder(stopOrder)
	if err != nil {
		return nil, err
	}

	// Add to stop book
	ob.backend.AppendToStopBook(stopOrder)

	// Create done object
	done = newDone(stopOrder)
	done.Stored = true

	quantity := stopOrder.Quantity()
	done.setLeftQuantity(&quantity)

	return done, nil
}

func (ob *OrderBook) checkOCO(order *Order, done *Done) bool {
	if order.OCO() == "" {
		return false
	}

	// Check if OCO order exists and cancel it
	ocoID := ob.backend.CheckOCO(order.ID())
	if ocoID == "" {
		return false
	}

	ocoOrder := ob.GetOrder(ocoID)
	if ocoOrder != nil {
		ob.CancelOrder(ocoID)
		done.appendCanceled(ocoOrder)
	}

	return false
}

// String implements fmt.Stringer interface
func (ob *OrderBook) String() string {
	builder := strings.Builder{}

	builder.WriteString("Ask:")
	asks := ob.backend.GetAsks()
	if stringer, ok := asks.(fmt.Stringer); ok {
		builder.WriteString(stringer.String())
	} else {
		builder.WriteString(" (No string representation available)")
	}
	builder.WriteString("\n")

	builder.WriteString("Bid:")
	bids := ob.backend.GetBids()
	if stringer, ok := bids.(fmt.Stringer); ok {
		builder.WriteString(stringer.String())
	} else {
		builder.WriteString(" (No string representation available)")
	}
	builder.WriteString("\n")

	return builder.String()
}

// Helper functions for the matching engine

// getOppositeOrders returns the orders from the opposite side of the book
func (ob *OrderBook) getOppositeOrders(side Side) interface{} {
	if side == Buy {
		return ob.backend.GetAsks() // For buy orders, get sell orders
	}
	return ob.backend.GetBids() // For sell orders, get buy orders
}

// oppositeOrder returns the opposite side
func oppositeOrder(side Side) Side {
	if side == Buy {
		return Sell
	}
	return Buy
}

// min returns the minimum of two decimals
func min(a, b fpdecimal.Decimal) fpdecimal.Decimal {
	if a.LessThan(b) {
		return a
	}
	return b
}

// isPriceMatching checks if the order price matches with the book price
func isPriceMatching(side Side, orderPrice, bookPrice fpdecimal.Decimal) bool {
	if side == Buy {
		// For buy orders, the order price must be >= the book price (sell price)
		return orderPrice.GreaterThanOrEqual(bookPrice)
	}
	// For sell orders, the order price must be <= the book price (buy price)
	return orderPrice.LessThanOrEqual(bookPrice)
}

// GetBids returns the bid side of the order book
func (ob *OrderBook) GetBids() interface{} {
	return ob.backend.GetBids()
}

// GetAsks returns the ask side of the order book
func (ob *OrderBook) GetAsks() interface{} {
	return ob.backend.GetAsks()
}

// Implement convertTrades function
func convertTrades(trades []TradeOrder) []messaging.Trade {
	converted := make([]messaging.Trade, len(trades))
	for i, trade := range trades {
		role := "MAKER"
		if trade.Role == TAKER {
			role = "TAKER"
		}
		converted[i] = messaging.Trade{
			OrderID:  trade.OrderID,
			Role:     role,
			Price:    trade.Price.String(),
			Quantity: trade.Quantity.String(),
			IsQuote:  trade.IsQuote,
		}
	}
	return converted
}
