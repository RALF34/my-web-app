from datetime import date

import streamlit as st

import queries
import visualization


def main():
    st.title("Daily air quality")
    st.write('''
            ### Select a place in France
            Identify the air quality monitoring station whose pollution data you are interested in.
            ''')
    st.session_state["available_stations"] = queries.get_stations()
    st.session_state["y-values"] = (None, None)
    st.session_data["no_data"] = False

    def update_values(station: str, pollutant: str) -> None:
        current_date = date.today()
        n_days = st.session_state["n_days"]
        working_days, weekends = queries.get_data(station, pollutant)
        counter = 0
        for i, data in enumerate([working_days, weekends]):
            if data:
                # Initialize "dictionary" which will contain the average
                # concentration values (set to zero when no data are
                # available) associated to the 24 hours of the day.
                dictionary = {str(x): float(0) for x in range(24)}
                for document in data:
                    # Get the hour of the day treated by the current document.
                    hour = document["_id"]["hour"]
                    # Extract only air concentration values being less than
                    # "n_days" days old.
                    history = list(zip(
                        document["history"]["values"],
                        document["history"]["dates"]))[::-1]
                    i = 0
                    limit = len(history)
                    while (
                        i < limit and 
                        (current_date-history[i][1]).days <= n_days):
                        i += 1
                    if i == limit:
                        i -= 1
                    # Update "dictionary".
                    dictionary[str(hour)] = \
                    float(mean([e[0] for e in history[:i]]))
                session_state["y-values"][i] = list(dictionary.values())
            else:
                counter += 1
        if counter == 2:
            st.session_state["no_data"] = True
    
    col1, col2 = st.columns((5,1))
    with col1:
        
        region = st.selectbox(
            "Select a French region",
            queries.get_items("regions", {}))
        
        department = st.selectbox(
            "Select a French department",
            queries.get_items("departments", {"_id": region}))
        
        city = st.selectbox(
            "Select a French city",
            queries.get_items("cities", {"_id": department}))
        
        available_stations = list(map(
            lambda x: x.split("#"),
            queries.get_items("stations", {"_id": city})))
        names = [e[0] for e in available_stations]
        codes = [e[1] for e in available_stations]
        station = st.selectbox("Select a station", names)
        if station not in st.session_state["available_stations"]:
            st.error("Sorry, no data available for this station.")
            st.stop()
        else:        
            pollutant = st.selectbox(
                "Select a type of pollution",
                queries.get_items("pollutants", {"_id": station}))
                
            strating_date, ending_date = queries.get_dates()
            station_code = codes[names.index(station)]
            n_days = st.slider(
                "When does the air pollution analysis start?",
                starting_date,
                ending_date,
                ending_date-timedelta(days=90),
                format="DD/MM/YY",
                key="n_days",
                on_change=update_values,
                args=(station_code, pollutant))
            
            update_values()
            if st.session_state["no_data"]:
                st.error("No pollution data are available for the given period.")
                st.stop()
            else:
                st.pyplot(
                    visualization.plot_variation(
                        st.session_state["y-values"],
                        pollutant,
                        station))
if __name__=="__main__":
    main()
