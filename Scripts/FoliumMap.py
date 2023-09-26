import folium
import pandas as pd
from folium.plugins import HeatMapWithTime

print("Finish importing packages")


def get_map(latitude=48.859553, longitude=2.336332):
    m = folium.Map(
        location=[latitude, longitude],
        zoom_start=12.5,  # type: ignore
        disable_interaction=True,
        control_scale=True,
        tiles=None,  # type: ignore
    )
    folium.raster_layers.TileLayer(tiles="openstreetmap", name="RITI TC").add_to(m)  # type: ignore
    return m


m = get_map()

riti_file = "./data/orig_dest_RITI_TC_riti_06_24_25_26_27_db.csv"
riti_tc = pd.read_csv(riti_file)
riti_tc["request_DateTime"] = pd.to_datetime(riti_tc["request_DateTime"])
riti_tc["request_DateTime"] = riti_tc["request_DateTime"].dt.round("60min")
riti_tc["request_DateTime"] = riti_tc["request_DateTime"].dt.tz_localize(None)

riti_count = (
    pd.DataFrame(
        riti_tc.groupby(["y", "x", "request_DateTime", "Source"]).size()
    )
    .reset_index()
    .rename(columns={0: "count"})
)
riti_count = riti_count.loc[riti_count["count"] > 1, :]

riti_count = riti_count.sort_values("request_DateTime")

# Extract the necessary columns from the DataFrame
data_or = riti_count.loc[
    riti_count["Source"] == "Origin", ["x", "y", "request_DateTime", "count"]
]
data_dest = riti_count.loc[
    riti_count["Source"] == "Destination",
    ["x", "y", "request_DateTime", "count"],
]

# Convert the request_DateTime column to string format
data_or["request_DateTime"] = data_or["request_DateTime"].astype(str)
data_dest["request_DateTime"] = data_dest["request_DateTime"].astype(str)

# Group the coordinates by time and create a list of lists
grouped_data_or = (
    data_or.groupby("request_DateTime")[["y", "x", "count"]]
    .apply(lambda x: x.values.tolist())
    .tolist()
)
grouped_data_dest = (
    data_dest.groupby("request_DateTime")[["y", "x", "count"]]
    .apply(lambda x: x.values.tolist())
    .tolist()
)

# Create a list of time values for the heatmap
time_values = data_or["request_DateTime"].unique().tolist()

custom_colors_or = ["#CCFFFF", "#99CCFF", "#6699FF", "#3366FF", "#0000FF"]
custom_colors_dest = ["#FFCCCC", "#FF9999", "#FF6666", "#FF3333", "#FF0000"]
color_positions = [0.0, 0.25, 0.5, 0.75, 1.0]
gradient_or = {
    pos: color for pos, color in zip(color_positions, custom_colors_or)
}
gradient_dest = {
    pos: color for pos, color in zip(color_positions, custom_colors_dest)
}

time_values = sorted(
    list(set(data_or["request_DateTime"]) | set(data_dest["request_DateTime"]))
)


heatmap_or = HeatMapWithTime(
    grouped_data_or,
    index=time_values,
    auto_play=True,
    min_opacity=0.15,  # type: ignore
    max_opacity=0.8,
    radius=5,
    gradient=gradient_or,
    overlay=True,
)

# Create a heatmap with animation using HeatMapWithTime for destinations
heatmap_dest = HeatMapWithTime(
    grouped_data_dest,
    index=time_values,
    auto_play=True,
    min_opacity=0.15,  # type: ignore
    max_opacity=0.8,
    radius=5,
    gradient=gradient_dest,
    overlay=True,
)

# Add both heatmaps to the map
heatmap_or.add_to(m)
heatmap_dest.add_to(m)

m.save("./data/map_idfm.html")
