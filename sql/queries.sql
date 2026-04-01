-----------------------------------------------------
-- Business questions for the NYC Taxi dataset
-----------------------------------------------------

-- 1. Total Revenue Over Time
-- How does revenue evolve over time?
SELECT
    d.year,
    d.month,
    d.day,
    SUM(f.total_amount) AS total_revenue
FROM fact_trips f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.day
ORDER BY d.year, d.month, d.day;

-- 2. Average Trip Distance by Passenger Count
-- Do more passengers mean longer trips?
SELECT
    f.passenger_count,
    AVG(f.trip_distance) AS avg_distance
FROM fact_trips f
GROUP BY f.passenger_count
ORDER BY f.passenger_count;

-- 3. Revenue by Payment Type
-- Which payment method generates more revenue?
SELECT
    p.payment_type,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_trip_value
FROM fact_trips f
JOIN dim_payment p ON f.payment_id = p.payment_id
GROUP BY p.payment_type
ORDER BY total_revenue DESC;

-- 4. Peak Demand (Trips per Weekday)
-- Which days have the highest demand?
SELECT
    d.weekday,
    COUNT(*) AS total_trips
FROM fact_trips f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.weekday
ORDER BY total_trips DESC;

-- 5. Tip Behavior Analysis
-- Do longer trips generate higher tips?
SELECT
    CASE
        WHEN f.trip_distance < 2 THEN 'Short'
        WHEN f.trip_distance < 5 THEN 'Medium'
        ELSE 'Long'
    END AS trip_category,
    AVG(f.tip_amount) AS avg_tip,
    AVG(f.total_amount) AS avg_total
FROM fact_trips f
GROUP BY trip_category;
