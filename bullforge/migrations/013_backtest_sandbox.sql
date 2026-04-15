CREATE SCHEMA IF NOT EXISTS backtest;

CREATE TABLE IF NOT EXISTS backtest.orders (LIKE public.orders INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.model_registry (LIKE public.model_registry INCLUDING ALL);
CREATE TABLE IF NOT EXISTS backtest.predictions (LIKE public.predictions INCLUDING ALL);
