from typing import List

import streamlit as st
from matplotlib import pyplot


AIR_POLLUTANTS = {
    "NO2": "nitrogen dioxide",
    "SO2": "sulphur dioxide",
    "PM2.5": "fine particles",
    "PM10": "particles",
    "CO": "carbone monoxide"
}

# https://www.who.int/news-room/feature-stories/detail/what-are-the-who-air-quality-guidelines
WHO_GUIDELINES = {
    pollutant: value for (pollutant, value) in zip(
        AIR_POLLUTANTS.keys(),
        [25,40,15,45,4]
    )
}

def plot_variation(
    values: List[List[float]],
    pollutant: str,
    station_name: str) -> pyplot.figure:
    '''
    Generate the graph showing average daily variation (obtained using
    average concentrations recorded at each of the 24 hours of the day, 
    stored in "values") of air concentration of "pollutant" recorded by 
    "station".
    '''
    figure, ax = pyplot.subplots()
    figure.set_size_inches(17,14)
    x = [str(x)+"h00" for x in range(24)]
    colors = ("dodgerblue","cyan")
    labels = ("Working_days","Week_end")

    def contains_zero(lists: List[List[float]]) -> bool:
        '''
        Return False if zero is not in any of the lists given by the
        "lists" argument, and True, along with the position of the
        list(s) containing zero, otherwise.
        '''
        answer, L = False, []
        for l in lists:
            for e in l:
                if not(e):
                    L.append(l.index(lists))
                    if not(answer):
                        answer = True
        return answer, L
        
    # For both of the lists given by "values", determine whether 
    # each of the 24 hours of the day is associated with an
    # average concentration value different from zero.
    hours_with_no_value, L = contains_zero(values)
    # Plot the data using either a continuous line (if all the 24
    # values are different from zero) or points.
    if not(hours_with_no_value):
        ax.plot(x, values[0], c="dodgerblue", label="Working days")
        ax.plot(x, values[1], c="cyan", label="Week-end")
    elif len(L)==1:
        i = 0 if 1 in L else 1
        ax.plot(x, values[i], c=colors[i], label=labels[i])
        ax.scatter(x, values[L[0]], c=colors[L[0]], label=labels[L[0]])
    else:
        ax.scatter(x, values[0], c="dodgerblue", label="Working days")
        ax.scatter(x, values[1], marker="s", c="cyan", label="Week-end")
    # Compute four threshold values based on the corresponding WHO
    # recommendation (will be used later to split the graph into
    # colored zones, improving readibility and understanding of the
    # displayed pollution data).
    thresholds = [
        (2*x/3)*WHO_recommendation[pollutant]
        for x in range(1,4)]
    ax.plot(
        range(24),
        [WHO_recommendation[pollutant]]*24,
        color="violet",
        ls="--",
        lw=1.7,
        label="Recommended daily \naverage (WHO)")
    # Set the upper bound value on the y-axis.
    default_bound = 2 * WHO_recommendation[pollutant]
    highest_value = max(values[0])
    upper_bound = default_bound if highest_value <= default_bound \
    else highest_value
    ax.set_ylim(0,upper_bound)
    # Split the graph into three colored zones.
    colors = ["limegreen","orange","red"]
    y_min = 0
    for j in range(3):
        ax.fill_between(
            list(range(24)),
            thresholds[j],
            y2=y_min,
            color=colors[j],
            alpha=0.1)
        y_min = thresholds[j]
    # Add a fourth zone if one or more values are above the highest
    # set threshold.
    if highest_value > thresholds[2]:
        ax.fill_between(
            list(range(24)),
            upper_bound,
            y2=thresholds[2],
            color="magenta",
            alpha=0.1)
    ax.set_ylabel(
        "Air"+" "*14+"\nconcentration"+" "*14+
        "\nof "+pollutant+" "*14+"\n("+
        ("µ" if pollutant != "CO" else "m")+"g/m³)"+" "*14,
        rotation="horizontal",
        size="large")
    ax.legend(loc="upper right")
    ax.set_title(
        "Average daily "+symbol_to_name[pollutant]+" pollution\n\
        recorded at :\n"+station_name,
        size="x-large",
        ha="center")
    return figure
