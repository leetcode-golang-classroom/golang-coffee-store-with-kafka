package application

func (app *App) SetUpRoutes() {
	orderHandler := NewHandler(app.broker)
	orderHandler.RegisterRoutes(app.mux)
}

func (app *App) SetupWorker() {
	orderWorker := NewWorker(app.broker)
	app.orderWorker = orderWorker
}
