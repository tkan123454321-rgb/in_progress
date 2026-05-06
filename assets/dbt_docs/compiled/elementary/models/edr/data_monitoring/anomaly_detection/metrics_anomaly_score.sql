

with
    data_monitoring_metrics as (select * from "lakehouse_main"."elementary"."data_monitoring_metrics"),

    time_window_aggregation as (

        select
            id,
            full_table_name,
            column_name,
            dimension,
            dimension_value,
            metric_name,
            metric_value,
            source_value,
            bucket_start,
            bucket_end,
            bucket_duration_hours,
            updated_at,
            avg(metric_value) over (
                partition by metric_name, full_table_name, column_name
                order by bucket_start asc
                rows between unbounded preceding and current row
            ) as training_avg,
            stddev(cast(metric_value as double)) over (
                partition by metric_name, full_table_name, column_name
                order by bucket_start asc
                rows between unbounded preceding and current row
            ) as training_stddev,
            count(metric_value) over (
                partition by metric_name, full_table_name, column_name
                order by bucket_start asc
                rows between unbounded preceding and current row
            ) as training_set_size,
            last_value(bucket_end) over (
                partition by metric_name, full_table_name, column_name
                order by bucket_start asc
                rows between unbounded preceding and current row
            ) training_end,
            first_value(bucket_end) over (
                partition by metric_name, full_table_name, column_name
                order by bucket_start asc
                rows between unbounded preceding and current row
            ) as training_start
        from data_monitoring_metrics
        group by
            id,
            full_table_name,
            column_name,
            dimension,
            dimension_value,
            metric_name,
            metric_value,
            source_value,
            bucket_start,
            bucket_end,
            bucket_duration_hours,
            updated_at
    ),

    time_window_scored as (

        select
            *,
            case
                when training_stddev is null
                then null
                when training_set_size = 1
                then null  -- Single value case - no historical context for anomaly detection
                when training_stddev = 0
                then 0  -- Stationary data case - valid, all values are identical
                else
                    (metric_value - training_avg)
                    / (training_stddev)
            end as anomaly_score
        from time_window_aggregation

    ),

    metrics_anomaly_score as (

        select
            id,
            full_table_name,
            column_name,
            dimension,
            dimension_value,
            metric_name,
            anomaly_score,
            metric_value as latest_metric_value,
            bucket_start,
            bucket_end,
            training_avg,
            training_stddev,
            training_start,
            training_end,
            training_set_size,
            max(updated_at) as updated_at
        from time_window_scored
        where
            metric_value is not null
            and training_avg is not null
            and bucket_end
            >= 
    date_add(
        'day',
        cast(-7 as integer),
        coalesce(
        try_cast(date_trunc('day', current_timestamp(6)) as  timestamp(6) ),
        cast(
            from_iso8601_timestamp(
                cast(date_trunc('day', current_timestamp(6)) as varchar)
            ) as  timestamp(6) 
        )
    )
    )

        group by
            id,
            full_table_name,
            column_name,
            dimension,
            dimension_value,
            metric_name,
            anomaly_score,
            metric_value,
            bucket_start,
            bucket_end,
            training_avg,
            training_stddev,
            training_start,
            training_end,
            training_set_size

    ),

    final as (

        select
            id,
            full_table_name,
            column_name,
            dimension,
            dimension_value,
            metric_name,
            anomaly_score,
            latest_metric_value,
            bucket_start,
            bucket_end,
            training_avg,
            training_stddev,
            training_start,
            training_end,
            training_set_size,
            updated_at,
            
    case
        when abs(anomaly_score) > 3
        then 
     true 

        else 
     false

    end
 as is_anomaly
        from metrics_anomaly_score
    )

select *
from final