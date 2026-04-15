package livetail

type FilterFunc func(any) bool

type Hub interface {
	Subscribe(teamID int64, ch chan any, filter FilterFunc) bool
	Unsubscribe(teamID int64, ch chan any)
	Publish(teamID int64, event any)
}
