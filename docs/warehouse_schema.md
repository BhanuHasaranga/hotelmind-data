# HotelMind AI — Warehouse Schema Reference

## Star Schema Entity Relationship (Text)

```
                              dim_date
                           ┌──────────────┐
                           │ date_key (PK)│
                           │ date         │
                           │ year         │
                           │ month        │
                           │ quarter      │
                           │ week_of_year │
                           │ day_name     │
                           │ is_weekend   │
                           │ is_holiday   │
                           └──────┬───────┘
                                  │ check_in/out_date_key, order_date_key, shift_date_key
            ┌─────────────────────┼────────────────────────┐
            ▼                     ▼                        ▼
    fact_booking        fact_restaurant_sale    fact_staff_attendance
    ┌─────────────┐     ┌─────────────────┐    ┌──────────────────┐
    │surrogate_key│     │surrogate_key    │    │surrogate_key     │
    │reservation_id│    │order_item_id    │    │attendance_id     │
    │room_key (FK)│     │menu_item_key(FK)│    │employee_key (FK) │
    │branch_key(FK│     │branch_key (FK)  │    │branch_key (FK)   │
    │hotel_key(FK)│     │hotel_key (FK)   │    │hotel_key (FK)    │
    │guest_key(FK)│     │order_date_key(FK│    │shift_date_key(FK)│
    │check_in_dk  │     │order_id         │    │scheduled_hours   │
    │check_out_dk │     │order_status     │    │actual_hours      │
    │nights       │     │quantity         │    │variance_hours    │
    │adults       │     │unit_price       │    │attendance_status │
    │total_amount │     │subtotal         │    └──────────────────┘
    │paid_amount  │     │price_variance   │
    │avg_daily_rate│    └─────────────────┘
    │is_terminal  │
    └─────────────┘
            │                                    fact_occupancy_daily
            │                                   ┌──────────────────────┐
            │                                   │surrogate_key         │
            │                                   │branch_key (FK)       │
            │                                   │hotel_key (FK)        │
            │                                   │date_key (FK)         │
            │                                   │total_rooms           │
            │                                   │occupied_rooms        │
            │                                   │available_rooms       │
            │                                   │occupancy_pct         │
            │                                   └──────────────────────┘
            │
   ┌────────┴─────────────────────────────────────┐
   ▼         ▼           ▼          ▼             ▼
dim_room  dim_branch  dim_hotel  dim_guest  dim_employee  dim_menu_item
```

---

## Dimension Tables

### dim_date
| Column | Type | Description |
|---|---|---|
| date_key | INTEGER | YYYYMMDD (PK) |
| date | DATE | Calendar date |
| year | INTEGER | Calendar year |
| quarter | INTEGER | 1–4 |
| month | INTEGER | 1–12 |
| month_name | VARCHAR | 'January' etc |
| week_of_year | INTEGER | ISO week number |
| day_of_month | INTEGER | 1–31 |
| day_of_week | INTEGER | 0=Sunday |
| day_name | VARCHAR | 'Monday' etc |
| is_weekend | BOOLEAN | True for Sat/Sun |
| is_holiday | BOOLEAN | Placeholder, always false in Phase 3 |
| fiscal_year | INTEGER | Same as calendar year |
| first_day_of_month | DATE | For MTD filters |
| last_day_of_month | DATE | For period-end labels |
| first_day_of_year | DATE | For YTD filters |

### dim_hotel
| Column | Type | Description |
|---|---|---|
| hotel_key | VARCHAR | Surrogate PK (MD5 hash) |
| hotel_id | UUID | Natural key from operational DB |
| hotel_name | VARCHAR | Hotel name |
| star_rating | INTEGER | 1–5 |
| city | VARCHAR | City |
| country | VARCHAR | Country |
| address | TEXT | Full address |
| is_active | BOOLEAN | Soft-delete flag |

### dim_branch
| Column | Type | Description |
|---|---|---|
| branch_key | VARCHAR | Surrogate PK |
| branch_id | UUID | Natural key |
| hotel_key | VARCHAR | FK → dim_hotel |
| hotel_id | UUID | Denormalised hotel UUID |
| hotel_name | VARCHAR | Denormalised hotel name |
| branch_name | VARCHAR | Branch name |
| city | VARCHAR | Branch city (falls back to hotel city) |
| country | VARCHAR | From parent hotel |
| is_main_branch | BOOLEAN | |

### dim_room
| Column | Type | Description |
|---|---|---|
| room_key | VARCHAR | Surrogate PK |
| room_id | UUID | Natural key |
| branch_key | VARCHAR | FK → dim_branch |
| hotel_name | VARCHAR | Denormalised |
| branch_name | VARCHAR | Denormalised |
| floor_number | INTEGER | Floor level |
| room_number | VARCHAR | e.g., "301" |
| room_type_name | VARCHAR | STANDARD, DELUXE, SUITE, PRESIDENTIAL |
| base_price | NUMERIC(10,2) | Daily rate |
| max_occupancy | INTEGER | Guest capacity |
| amenity_names | TEXT | Pipe-delimited: "WiFi \| AC \| TV" |
| amenity_count | INTEGER | Count of amenities |
| current_status | VARCHAR | AVAILABLE, OCCUPIED, MAINTENANCE, CLEANING |
| is_active | BOOLEAN | |

### dim_guest
| Column | Type | Description |
|---|---|---|
| guest_key | VARCHAR | Surrogate PK |
| guest_id | UUID | Natural key |
| first_name | VARCHAR | |
| last_name | VARCHAR | |
| full_name | VARCHAR | first + last |
| email | VARCHAR | Lowercased |
| phone | VARCHAR | |
| nationality | VARCHAR | |
| lifetime_bookings | INTEGER | Computed at dbt run time |
| lifetime_spend | NUMERIC(12,2) | Total revenue from non-terminal reservations |
| first_stay_date | DATE | Earliest check-in |
| last_stay_date | DATE | Most recent check-in |

### dim_employee
| Column | Type | Description |
|---|---|---|
| employee_key | VARCHAR | Surrogate PK |
| employee_id | UUID | Natural key |
| branch_key | VARCHAR | FK → dim_branch |
| department_id | UUID | Natural FK |
| department_name | VARCHAR | Denormalised |
| full_name | VARCHAR | |
| email | VARCHAR | |
| role | VARCHAR | Job title |
| hire_date | DATE | |
| tenure_years | INTEGER | Approximate years |
| is_active | BOOLEAN | |

### dim_menu_item
| Column | Type | Description |
|---|---|---|
| menu_item_key | VARCHAR | Surrogate PK |
| menu_item_id | UUID | Natural key |
| branch_key | VARCHAR | FK → dim_branch |
| category_name | VARCHAR | Denormalised food category |
| item_name | VARCHAR | Dish name |
| current_price | NUMERIC(10,2) | Current menu price |
| is_available | BOOLEAN | |

---

## Fact Tables

### fact_booking
**Grain:** One row per reservation
**Incremental:** Yes (on updated_at)

| Column | Type | Description |
|---|---|---|
| surrogate_key | VARCHAR | PK (hash of reservation_id) |
| reservation_id | UUID | Degenerate dimension |
| reservation_status | VARCHAR | PENDING, CONFIRMED, etc |
| room_key | VARCHAR | FK → dim_room |
| branch_key | VARCHAR | FK → dim_branch |
| hotel_key | VARCHAR | FK → dim_hotel |
| guest_key | VARCHAR | FK → dim_guest |
| check_in_date_key | INTEGER | FK → dim_date |
| check_out_date_key | INTEGER | FK → dim_date |
| nights | INTEGER | check_out - check_in |
| adults | INTEGER | |
| children | INTEGER | |
| total_amount | NUMERIC(10,2) | Booking total |
| paid_amount | NUMERIC(10,2) | Amount received |
| outstanding_amount | NUMERIC(10,2) | total - paid |
| avg_daily_rate | NUMERIC(10,2) | total / nights |
| is_terminal | BOOLEAN | CANCELLED or NO_SHOW |
| is_completed | BOOLEAN | CHECKED_OUT |

### fact_restaurant_sale
**Grain:** One row per order line item
**Incremental:** Yes (on updated_at)

| Column | Type | Description |
|---|---|---|
| surrogate_key | VARCHAR | PK (hash of order_item_id) |
| order_item_id | UUID | Degenerate dimension |
| order_id | UUID | Degenerate dimension |
| order_status | VARCHAR | OPEN, CLOSED, CANCELLED |
| menu_item_key | VARCHAR | FK → dim_menu_item |
| branch_key | VARCHAR | FK → dim_branch |
| hotel_key | VARCHAR | FK → dim_hotel |
| order_date_key | INTEGER | FK → dim_date |
| quantity | INTEGER | Items ordered |
| unit_price | NUMERIC(10,2) | Price at time of order |
| subtotal | NUMERIC(10,2) | quantity × unit_price |
| price_variance | NUMERIC(10,2) | unit_price vs current menu price |

### fact_occupancy_daily
**Grain:** One row per branch per calendar date
**Materialization:** Table (full rebuild — O(branches × 730 days))

| Column | Type | Description |
|---|---|---|
| surrogate_key | VARCHAR | PK (hash of branch_id + date_key) |
| branch_key | VARCHAR | FK → dim_branch |
| hotel_key | VARCHAR | FK → dim_hotel |
| date_key | INTEGER | FK → dim_date |
| occupancy_date | DATE | |
| total_rooms | INTEGER | Active rooms in branch |
| occupied_rooms | INTEGER | Rooms with active reservation that day |
| available_rooms | INTEGER | total - occupied |
| occupancy_pct | NUMERIC(5,2) | occupied / total × 100 |

### fact_staff_attendance
**Grain:** One row per attendance record
**Incremental:** Yes (on updated_at)

| Column | Type | Description |
|---|---|---|
| surrogate_key | VARCHAR | PK |
| attendance_id | UUID | Degenerate dimension |
| attendance_status | VARCHAR | PRESENT, ABSENT, LATE, HALF_DAY |
| employee_key | VARCHAR | FK → dim_employee |
| branch_key | VARCHAR | FK → dim_branch |
| hotel_key | VARCHAR | FK → dim_hotel |
| shift_date_key | INTEGER | FK → dim_date |
| scheduled_hours | NUMERIC(5,2) | From schedule |
| actual_hours | NUMERIC(5,2) | From attendance clock times |
| variance_hours | NUMERIC(5,2) | actual - scheduled |
| is_present | BOOLEAN | |
| is_late | BOOLEAN | |

---

## Mart Tables

### mart_revenue_daily
**Grain:** branch_id × date | **For:** BI dashboards, Phase 4 revenue forecasting

Key columns: `total_revenue`, `room_revenue`, `fb_revenue`, `avg_daily_rate`, `revpar`,
`occupancy_pct`, `revenue_7day_avg`, `revenue_mtd`, `revenue_ytd`

### mart_occupancy_daily
**Grain:** branch_id × date | **For:** Phase 4 occupancy forecasting ML model

Key columns: `occupancy_pct`, `occupancy_7day_avg`, `occupancy_30day_avg`,
`occupancy_pct_lag_7d`, `occupancy_pct_lag_365d`, `occupancy_mtd_avg`

### mart_restaurant_daily
**Grain:** branch_id × date | **For:** Phase 4 restaurant demand forecasting

Key columns: `total_orders`, `items_sold`, `total_revenue`, `avg_order_value`,
`breakfast_revenue`, `lunch_revenue`, `dinner_revenue`, `revenue_7day_avg`

### mart_staff_daily
**Grain:** branch_id × department_id × date | **For:** Phase 4 staff optimization

Key columns: `scheduled_employees`, `present_employees`, `attendance_rate_pct`,
`total_actual_hours`, `hours_utilisation_pct`
