# CloudWatch dashboard mirroring the local Grafana board. MSF publishes every
# Flink metric (built-in and custom demo* metrics) to AWS/KinesisAnalytics at
# OPERATOR level. SEARCH expressions keep widgets robust to operator naming.
locals {
  dash_widget = { for i, w in [
    {
      title = "Records/sec out — by operator"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Operator} MetricName=\"numRecordsOutPerSecond\" Application=\"${var.project}\"', 'Average')"
    },
    {
      title = "Records out (total) — by operator"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Operator} MetricName=\"numRecordsOut\" Application=\"${var.project}\"', 'Maximum')"
    },
    {
      title = "Volume in — bytes/sec per parser"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Operator} MetricName=\"demoBytesInPerSecond\" Application=\"${var.project}\"', 'Average')"
    },
    {
      title = "Volume out — bytes/sec per sink"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Operator} MetricName=\"demoBytesOutPerSecond\" Application=\"${var.project}\"', 'Average')"
    },
    {
      title = "Duplicates dropped"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Operator} MetricName=\"demoDuplicatesDropped\" Application=\"${var.project}\"', 'Maximum')"
    },
    {
      title = "Busy time ms/sec (1000 = saturated)"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Task} MetricName=\"busyTimeMsPerSecond\" Application=\"${var.project}\"', 'Average')"
    },
    {
      title = "Backpressured time ms/sec"
      query = "SEARCH('{AWS/KinesisAnalytics,Application,Task} MetricName=\"backPressuredTimeMsPerSecond\" Application=\"${var.project}\"', 'Average')"
    },
    {
      title = "Checkpoint duration (ms)"
      query = "SEARCH('{AWS/KinesisAnalytics,Application} MetricName=\"lastCheckpointDuration\" Application=\"${var.project}\"', 'Maximum')"
    },
  ] : i => w }
}

resource "aws_cloudwatch_dashboard" "this" {
  dashboard_name = var.project
  dashboard_body = jsonencode({
    widgets = [for i, w in local.dash_widget : {
      type   = "metric"
      x      = (tonumber(i) % 2) * 12
      y      = floor(tonumber(i) / 2) * 6
      width  = 12
      height = 6
      properties = {
        title   = w.title
        region  = var.region
        view    = "timeSeries"
        stacked = false
        metrics = [[{ expression = w.query, id = "e${i}" }]]
        period  = 60
      }
    }]
  })
}
