select ticker, min(event_date) from bronze.historical_quotes
group by ticker
order by min(event_date) asc

