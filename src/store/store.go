package store

import (
	"github.com/itchyny/gojq"
)

type Store interface {
	/*
	 *	id - Name of the window
	 *  t - unix timestamp the metric occoured
	 * 	metrc - metric to add using lambda
	 *	lamba - f(current_state, new_metric) new_state
	 */
	Push(id string, t int64, metric any, lambda *gojq.Code)

	/*
	 *	t - current unix timestamp
	 */
	Tick(t int64)

	/*
	 * returns a representation of the store, and the current store time
	 */
	Get_representation() (map[string]any, int64)
}

type Store_factory interface {
	New()
}
