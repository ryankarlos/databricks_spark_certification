

from pyspark.sql.functions import col

limitEventsDF = (df
  .filter(col("event_name") != "reviews")
  .filter(col("event_name") != "checkout")
  .filter(col("event_name") != "register")
  .filter(col("event_name") != "email_coupon")
  .filter(col("event_name") != "cc_info")
  .filter(col("event_name") != "delivery")
  .filter(col("event_name") != "shipping_info")
  .filter(col("event_name") != "press")
)

limitEventsDF.count()

# Prints the plans (logical and physical), optionally formatted by a given explain mode
limitEventsDF.explain(True)

betterDF = (df.filter(
  (col("event_name").isNotNull()) &
  (col("event_name") != "reviews") &
  (col("event_name") != "checkout") &
  (col("event_name") != "register") &
  (col("event_name") != "email_coupon") &
  (col("event_name") != "cc_info") &
  (col("event_name") != "delivery") &
  (col("event_name") != "shipping_info") &
  (col("event_name") != "press")
))

betterDF.count()

betterDF.explain(True)


stupidDF = (df
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
)

stupidDF.explain(True)