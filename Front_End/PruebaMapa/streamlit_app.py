# -*- coding: utf-8 -*-
# Copyright 2018-2022 Streamlit Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example of showing geographic data."""

import os
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE AND FAVICON
st.set_page_config(
    layout="wide", page_title="Valencia Pickups Demo", page_icon="🚗"
)

# LOAD DATA ONCE (Replace this with your Valencia pickup data)
@st.cache
def load_data():
    # Load your Valencia pickup data (latitude and longitude coordinates)
    # Replace this with your data source or data loading logic
    data = pd.DataFrame({
        "lat": [39.46975, 39.469, 39.47],  # Example latitude data
        "lon": [-0.37739, -0.376, -0.378],  # Example longitude data
    })
    return data

# STREAMLIT APP LAYOUT
data = load_data()

# LAYING OUT THE APP
st.title("Valencia Pickups Demo")
st.write(
    """
    ##
    Displaying pickup locations in Valencia, Spain.
    """
)

# Create a Pydeck map
st.pydeck_chart(
    pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state={
            "latitude": 39.46975,  # Latitude of Valencia
            "longitude": -0.37739,  # Longitude of Valencia
            "zoom": 12,
            "pitch": 50,
        },
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=data,
                get_position=["lon", "lat"],
                get_radius=100,
                get_fill_color=[255, 0, 0],  # Red color for points
                opacity=0.8,
                pickable=True,
            ),
        ],
    )
)
