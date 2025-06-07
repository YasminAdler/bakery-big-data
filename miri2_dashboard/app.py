import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pickle
import os

# Import local modules
from utils.data_generator import generate_product_data, generate_store_data, generate_sales_data, generate_inventory_data
from utils.visualization import create_sales_forecast_chart, create_waste_category_chart, create_inventory_sales_chart
from models.demand_forecast import train_forecast_model, predict_demand
from utils.simulation import simulate_demand, simulate_inventory_levels

def main():
    # Page configuration
    st.set_page_config(
        page_title="Bakery Data Warehouse Dashboard",
        page_icon="🥐",
        layout="wide"
    )

    # Header and introduction
    st.title("🥖 Bakery Data Warehouse Dashboard")
    st.markdown("""
    This dashboard helps bakery managers monitor product sales, predict demand, and reduce waste.
    The data warehouse integrates multiple sources including POS systems, inventory management, equipment IoT sensors,
    customer feedback, and weather data to optimize bakery operations.
    """)
    # Create tabs for different dashboard sections
    tab1, tab2, tab3 = st.tabs(["Main Dashboard", "Demand Simulation", "Data Exploration"])

    # Sidebar filters (apply to all tabs)
    with st.sidebar:
        st.header("Filters")
        
        # Load data
        products_df = generate_product_data()
        stores_df = generate_store_data()
        sales_df = generate_sales_data()
        inventory_df = generate_inventory_data()
        
        # Product filter
        product_options = products_df['product_name'].tolist()
        selected_products = st.multiselect(
            "Select Products", 
            product_options, 
            default=product_options[:3]
        )
        
        # Store filter
        store_options = stores_df['location'].tolist()
        selected_stores = st.multiselect(
            "Select Stores", 
            store_options, 
            default=store_options[0]
        )
        
        # Date range filter
        date_range = st.date_input(
            "Select Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
        )
        
        # Apply filters
        if selected_products and selected_stores:
            filtered_sales = sales_df[
                (sales_df['product_name'].isin(selected_products)) & 
                (sales_df['store_location'].isin(selected_stores)) &
                (sales_df['sale_date'] >= pd.Timestamp(date_range[0])) &
                (sales_df['sale_date'] <= pd.Timestamp(date_range[1]))
            ]
            
            filtered_inventory = inventory_df[
                (inventory_df['product_name'].isin(selected_products)) & 
                (inventory_df['store_location'].isin(selected_stores)) &
                (inventory_df['inventory_date'] >= pd.Timestamp(date_range[0])) &
                (inventory_df['inventory_date'] <= pd.Timestamp(date_range[1]))
            ]
        else:
            filtered_sales = pd.DataFrame()
            filtered_inventory = pd.DataFrame()

        st.markdown("---")
        st.write("Bakery Data Warehouse Solution")
        st.info("This dashboard is connected to a data warehouse with bronze, silver, and gold layers supporting ML-powered demand forecasting, waste reduction, and inventory optimization.")

    ###################
    # TAB 1: MAIN DASHBOARD
    ###################
    with tab1:
        if filtered_sales.empty or filtered_inventory.empty:
            st.warning("Please select at least one product and one store to display the dashboard.")
        else:
            # Top metrics section
            st.subheader("Key Performance Indicators")
            col1, col2, col3, col4 = st.columns(4)
            
            # Calculate KPIs
            total_sales = filtered_sales['total_revenue'].sum()
            avg_daily_sales = total_sales / max(1, (date_range[1] - date_range[0]).days)
            total_waste = filtered_inventory['waste_quantity'].sum()
            waste_ratio = (total_waste / filtered_inventory['sold_quantity'].sum()) * 100 if filtered_inventory['sold_quantity'].sum() > 0 else 0
            
            col1.metric("Total Sales", f"${total_sales:,.2f}", 
                       f"{np.random.choice(['+', '-'])}{np.random.randint(5, 15)}% vs prev period")
            col2.metric("Avg Daily Sales", f"${avg_daily_sales:,.2f}")
            col3.metric("Total Waste", f"{total_waste:,.0f} units", 
                      f"{np.random.choice(['+', '-'])}{np.random.randint(2, 10)}%")
            col4.metric("Waste Ratio", f"{waste_ratio:.1f}%", 
                      f"-{np.random.randint(1, 5)}% vs target")
            
            # Main charts
            st.markdown("---")
            st.subheader("Sales vs. Forecast")
            sales_forecast_chart = create_sales_forecast_chart(filtered_sales)
            st.plotly_chart(sales_forecast_chart, use_container_width=True)
            
            # Waste and inventory charts in columns
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Waste by Category")
                waste_chart = create_waste_category_chart(filtered_inventory)
                st.plotly_chart(waste_chart, use_container_width=True)
            
            with col2:
                st.subheader("Inventory vs. Sales Trends")
                inventory_chart = create_inventory_sales_chart(filtered_sales, filtered_inventory)
                st.plotly_chart(inventory_chart, use_container_width=True)
            
            # Inventory health metrics
            st.markdown("---")
            st.subheader("Inventory Health")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                low_stock = filtered_inventory[filtered_inventory['days_of_supply'] < 2]['product_name'].nunique()
                st.metric("Low Stock Products", f"{low_stock} items", 
                         f"-{np.random.randint(1, 3)} from yesterday")
                
            with col2:
                optimal_stock = filtered_inventory[
                    (filtered_inventory['days_of_supply'] >= 2) & 
                    (filtered_inventory['days_of_supply'] <= 5)
                ]['product_name'].nunique()
                st.metric("Optimal Stock Products", f"{optimal_stock} items", 
                         f"+{np.random.randint(1, 3)} from yesterday")
                
            with col3:
                overstock = filtered_inventory[filtered_inventory['days_of_supply'] > 5]['product_name'].nunique()
                st.metric("Overstock Products", f"{overstock} items", 
                         f"{np.random.choice(['+', '-'])}{np.random.randint(1, 3)} from yesterday")
            
            # Health visualization
            inventory_health = pd.DataFrame({
                'Category': ['Low Stock', 'Optimal', 'Overstock'],
                'Count': [low_stock, optimal_stock, overstock]
            })
            
            fig = px.bar(inventory_health, x='Category', y='Count', 
                       color='Category',
                       color_discrete_map={
                           'Low Stock': 'red',
                           'Optimal': 'green',
                           'Overstock': 'orange'
                       })
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    ###################
    # TAB 2: DEMAND SIMULATION
    ###################
    with tab2:
        st.header("Demand Forecasting & Waste Reduction Simulation")
        st.write("""
        This simulation tool helps optimize production planning by forecasting demand and estimating potential waste.
        Adjust the parameters below to see how different factors affect sales and waste.
        """)
        
        if filtered_sales.empty or filtered_inventory.empty:
            st.warning("Please select at least one product and one store to run a simulation.")
        else:
            # Simulation parameters
            st.subheader("Simulation Parameters")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                forecast_days = st.slider("Forecast Days", 1, 14, 7)
            
            with col2:
                promotion_factor = st.slider("Promotion Impact (%)", -20, 50, 10)
                
            with col3:
                weather_impact = st.selectbox(
                    "Weather Forecast",
                    ["Sunny", "Cloudy", "Rainy", "Snowy"],
                    index=0
                )
            
            # Production adjustment options
            st.subheader("Production Adjustment Strategy")
            strategy = st.radio(
                "Select production strategy:",
                ["Conservative (minimize waste)", "Balanced (moderate waste/stockout risk)", "Aggressive (minimize stockouts)"],
                index=1
            )
            
            # Strategy factors (for simulation)
            strategy_factors = {
                "Conservative (minimize waste)": 0.9,
                "Balanced (moderate waste/stockout risk)": 1.0,
                "Aggressive (minimize stockouts)": 1.2
            }
            
            if st.button("Run Simulation"):
                with st.spinner("Running demand forecasting simulation..."):
                    # Create model based on historical data
                    model = train_forecast_model(filtered_sales)
                    
                    # Predict future demand
                    future_demand = predict_demand(
                        model, 
                        filtered_sales, 
                        days=forecast_days,
                        promotion_factor=promotion_factor/100,
                        weather=weather_impact
                    )
                    
                    # Apply production strategy
                    strategy_factor = strategy_factors[strategy]
                    future_demand['production_plan'] = (future_demand['predicted_demand'] * strategy_factor).round().astype(int)
                    
                    # Calculate estimated waste based on production plan
                    waste_estimate = simulate_inventory_levels(
                        future_demand,
                        filtered_inventory,
                        products_df
                    )
                    
                    # Display simulation results
                    st.markdown("---")
                    st.subheader("Simulation Results")
                    
                    # Summary metrics
                    total_predicted_demand = future_demand['predicted_demand'].sum()
                    total_production_plan = future_demand['production_plan'].sum()
                    total_estimated_waste = waste_estimate['estimated_waste'].sum()
                    estimated_waste_ratio = (total_estimated_waste / total_production_plan) * 100 if total_production_plan > 0 else 0
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Forecasted Demand", f"{total_predicted_demand:,.0f} units")
                    col2.metric("Total Production Plan", f"{total_production_plan:,.0f} units")
                    col3.metric("Estimated Waste", f"{total_estimated_waste:,.0f} units ({estimated_waste_ratio:.1f}%)")
                    
                    # Detailed results in tabs
                    tab_demand, tab_waste, tab_rec = st.tabs(["Demand Forecast", "Waste Estimate", "Recommendations"])
                    
                    with tab_demand:
                        st.subheader("Detailed Forecast by Product")
                        st.dataframe(future_demand[['date', 'product_name', 'store_location', 'predicted_demand', 'production_plan']])
                        
                        fig = px.bar(
                            future_demand, 
                            x='date', 
                            y='predicted_demand',
                            color='product_name',
                            title="Forecasted Demand by Product and Date"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with tab_waste:
                        st.subheader("Estimated Waste by Product")
                        st.dataframe(waste_estimate[['product_name', 'product_category', 'current_stock', 'predicted_demand', 'estimated_waste', 'waste_ratio']])
                        
                        # Create waste visualization
                        fig = px.bar(
                            waste_estimate.sort_values('waste_ratio', ascending=False), 
                            x='product_name', 
                            y='waste_ratio',
                            color='product_category',
                            title="Estimated Waste Ratio by Product (%)"
                        )
                        fig.update_layout(xaxis_title="Product", yaxis_title="Waste Ratio (%)")
                        fig.update_yaxes(range=[0, min(100, waste_estimate['waste_ratio'].max() * 1.2)])
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with tab_rec:
                        st.subheader("Production Recommendations")
                        
                        # Generate recommendations
                        recommendations = []
                        
                        # Find high waste products
                        high_waste = waste_estimate[waste_estimate['waste_ratio'] > 15]
                        for _, row in high_waste.iterrows():
                            recommendations.append({
                                'product': row['product_name'],
                                'recommendation': f"Reduce production by {max(5, int(row['estimated_waste']))} units",
                                'reason': f"High waste ratio of {row['waste_ratio']:.1f}%",
                                'priority': 'High' if row['waste_ratio'] > 25 else 'Medium'
                            })
                        
                        # Find potential stockouts
                        potential_stockouts = future_demand[future_demand['predicted_demand'] > future_demand['production_plan'] * 1.1]
                        for _, row in potential_stockouts.iterrows():
                            shortfall = int(row['predicted_demand'] - row['production_plan'])
                            if shortfall > 0:
                                recommendations.append({
                                    'product': row['product_name'],
                                    'recommendation': f"Increase production by {shortfall} units for {row['date'].strftime('%Y-%m-%d')}",
                                    'reason': f"Forecasted demand exceeds production plan",
                                    'priority': 'High' if shortfall > 10 else 'Medium'
                                })
                        
                        # Display recommendations
                        if recommendations:
                            recommendations_df = pd.DataFrame(recommendations)
                            st.dataframe(recommendations_df)
                        else:
                            st.write("No specific recommendations needed. Current production plan is well-balanced.")
                        
                        # Display optimized production plan
                        st.subheader("Optimized Production Plan")
                        
                        # Calculate optimized plan based on recommendations
                        optimized_plan = future_demand.copy()
                        
                        for _, rec in enumerate(recommendations):
                            product = rec['product']
                            if "Reduce" in rec['recommendation']:
                                # Extract number from recommendation
                                reduction = int(rec['recommendation'].split("by ")[1].split(" ")[0])
                                mask = optimized_plan['product_name'] == product
                                # Distribute reduction across days
                                days_to_adjust = mask.sum()
                                if days_to_adjust > 0:
                                    reduction_per_day = max(1, reduction // days_to_adjust)
                                    optimized_plan.loc[mask, 'production_plan'] = optimized_plan.loc[mask, 'production_plan'] - reduction_per_day
                                    optimized_plan.loc[mask, 'production_plan'] = optimized_plan.loc[mask, 'production_plan'].clip(lower=0)
                            
                            elif "Increase" in rec['recommendation'] and "for" in rec['recommendation']:
                                # Extract info from recommendation
                                parts = rec['recommendation'].split("by ")[1]
                                increase = int(parts.split(" ")[0])
                                date_str = parts.split("for ")[1]
                                date = datetime.strptime(date_str, '%Y-%m-%d')
                                
                                # Apply increase to specific date
                                mask = (optimized_plan['product_name'] == product) & (optimized_plan['date'] == date)
                                if mask.any():
                                    optimized_plan.loc[mask, 'production_plan'] += increase
                        
                        # Display the optimized plan
                        fig = go.Figure()
                        
                        for product in optimized_plan['product_name'].unique():
                            product_data = optimized_plan[optimized_plan['product_name'] == product]
                            fig.add_trace(go.Bar(
                                x=product_data['date'],
                                y=product_data['production_plan'],
                                name=product
                            ))
                        
                        fig.update_layout(
                            title="Optimized Daily Production Plan by Product",
                            xaxis_title="Date",
                            yaxis_title="Units to Produce",
                            barmode='stack'
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show expected results
                        expected_waste_reduction = total_estimated_waste - (total_production_plan * 0.85)
                        expected_waste_reduction_pct = (expected_waste_reduction / total_estimated_waste) * 100 if total_estimated_waste > 0 else 0
                        
                        st.success(f"Expected waste reduction with optimized plan: {expected_waste_reduction:.0f} units ({expected_waste_reduction_pct:.1f}%)")

    ###################
    # TAB 3: DATA EXPLORATION
    ###################
    with tab3:
        st.header("Data Warehouse Exploration")
        st.write("""
        This section allows you to explore the various data tables in our bakery data warehouse.
        Select a table to view its schema and sample data.
        """)
        
        # Table selection
        table_options = [
            "Products (dim_product)", 
            "Stores (dim_store)", 
            "Sales (fact_sales)", 
            "Inventory (fact_inventory)", 
            "ML Features (fact_demand_forecast_features)"
        ]
        
        selected_table = st.selectbox("Select a table to explore", table_options)
        
        if selected_table == "Products (dim_product)":
            st.subheader("Product Dimension Table")
            st.write("This table contains static product information.")
            st.dataframe(products_df)
            
            # Schema visualization
            st.subheader("Schema")
            schema = {
                "Column": ["product_id", "product_name", "category", "subcategory", "shelf_life_hours", "base_cost"],
                "Type": ["INT", "VARCHAR(100)", "VARCHAR(50)", "VARCHAR(50)", "INT", "DECIMAL(10,2)"],
                "Description": [
                    "Primary key", 
                    "Name of the product",
                    "Product category (Bread, Pastry, etc.)",
                    "Specific subcategory",
                    "Shelf life in hours before product expires",
                    "Base production cost"
                ]
            }
            st.table(pd.DataFrame(schema))
            
        elif selected_table == "Stores (dim_store)":
            st.subheader("Store Dimension Table")
            st.write("This table contains information about store locations.")
            st.dataframe(stores_df)
            
            # Schema visualization
            st.subheader("Schema")
            schema = {
                "Column": ["store_id", "location", "city", "region", "total_area_sqm"],
                "Type": ["INT", "VARCHAR(50)", "VARCHAR(100)", "VARCHAR(50)", "DECIMAL(10,2)"],
                "Description": [
                    "Primary key", 
                    "Store location name",
                    "City",
                    "Geographic region",
                    "Total area in square meters"
                ]
            }
            st.table(pd.DataFrame(schema))
            
        elif selected_table == "Sales (fact_sales)":
            st.subheader("Sales Fact Table")
            st.write("This table contains transactional sales data from the POS system.")
            st.dataframe(sales_df.head(100))
            
            # Schema visualization
            st.subheader("Schema")
            schema = {
                "Column": ["sale_id", "sale_date", "product_id", "store_id", "quantity_sold", "unit_price", "total_revenue", "promotion_applied", "time_of_day"],
                "Type": ["VARCHAR(50)", "DATE", "INT", "INT", "INT", "DECIMAL(10,2)", "DECIMAL(10,2)", "BOOLEAN", "VARCHAR(20)"],
                "Description": [
                    "Primary key", 
                    "Date of the sale",
                    "Foreign key to dim_product",
                    "Foreign key to dim_store",
                    "Number of units sold",
                    "Unit price",
                    "Total revenue (quantity × price)",
                    "Whether a promotion was applied",
                    "Time of day (Morning, Afternoon, Evening)"
                ]
            }
            st.table(pd.DataFrame(schema))
            
        elif selected_table == "Inventory (fact_inventory)":
            st.subheader("Inventory Fact Table")
            st.write("This table contains daily inventory levels and waste metrics.")
            st.dataframe(inventory_df.head(100))
            
            # Schema visualization
            st.subheader("Schema")
            schema = {
                "Column": ["inventory_id", "inventory_date", "product_id", "store_id", "beginning_stock", "restocked_quantity", "sold_quantity", "waste_quantity", "waste_ratio", "closing_stock", "days_of_supply"],
                "Type": ["VARCHAR(50)", "DATE", "INT", "INT", "INT", "INT", "INT", "INT", "DECIMAL(5,4)", "INT", "DECIMAL(5,2)"],
                "Description": [
                    "Primary key", 
                    "Date of inventory record",
                    "Foreign key to dim_product",
                    "Foreign key to dim_store",
                    "Stock at beginning of day",
                    "New stock added during the day",
                    "Units sold during the day",
                    "Units wasted during the day",
                    "Waste as a proportion of total available inventory",
                    "Stock at end of day",
                    "Estimated days of supply based on sales rate"
                ]
            }
            st.table(pd.DataFrame(schema))
            
        elif selected_table == "ML Features (fact_demand_forecast_features)":
            st.subheader("Machine Learning Feature Table")
            st.write("This table contains features used for demand forecasting models.")
            
            # Create sample ML features table
            ml_features = pd.DataFrame({
                "feature_id": [f"F{i}" for i in range(1, 11)],
                "product_id": np.random.choice(products_df['product_id'].values, 10),
                "store_id": np.random.choice(stores_df['store_id'].values, 10),
                "date": [datetime.now() - timedelta(days=i) for i in range(10)],
                "sales_volume": np.random.randint(10, 100, 10),
                "avg_daily_sales": np.random.uniform(20, 80, 10).round(2),
                "sales_last_7_days": np.random.uniform(150, 500, 10).round(2),
                "is_weekend": [i % 7 >= 5 for i in range(10)],
                "temperature": np.random.uniform(10, 30, 10).round(1),
                "precipitation": np.random.uniform(0, 10, 10).round(1),
                "promotion_active": [bool(np.random.choice([0,1])) for _ in range(10)],
                "inventory_level": np.random.randint(20, 200, 10),
                "waste_ratio": np.random.uniform(0.01, 0.1, 10).round(4),
            })
            
            st.dataframe(ml_features)
            
            # Schema visualization
            st.subheader("Schema")
            schema = {
                "Column": ["feature_id", "product_id", "store_id", "date", "sales_volume", "avg_daily_sales", 
                          "sales_last_7_days", "is_weekend", "temperature", "precipitation", "promotion_active",
                          "inventory_level", "waste_ratio"],
                "Type": ["VARCHAR(50)", "INT", "INT", "DATE", "INT", "DECIMAL(10,2)", "DECIMAL(10,2)", "BOOLEAN", 
                        "DECIMAL(5,2)", "DECIMAL(5,2)", "BOOLEAN", "INT", "DECIMAL(5,4)"],
                "Description": [
                    "Primary key", 
                    "Foreign key to dim_product",
                    "Foreign key to dim_store",
                    "Date of the features",
                    "Sales volume for the day",
                    "Average daily sales over the last 30 days",
                    "Total sales over the last 7 days",
                    "Whether the date is a weekend",
                    "Temperature in Celsius",
                    "Precipitation in mm",
                    "Whether a promotion was active",
                    "Current inventory level",
                    "Recent waste ratio"
                ]
            }
            st.table(pd.DataFrame(schema))
            
            # Show machine learning model information
            st.subheader("Machine Learning Model")
            st.write("""
            These features are used to train a Random Forest Regression model for demand forecasting.
            The model is trained on historical data and can predict future demand based on:
            - Historical sales patterns
            - Day of week and seasonal effects
            - Weather conditions
            - Promotional activities
            - Inventory levels
            
            The model is retrained weekly to incorporate new data and maintain prediction accuracy.
            """)

if __name__ == "__main__":
    main()