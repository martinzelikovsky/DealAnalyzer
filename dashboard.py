import os
import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="Deal Analyzer Dashboard", layout="wide")

st.title("Deal Analyzer Interactive Dashboard")

results_dir = Path("results")
if not results_dir.exists():
    st.error("No results directory found.")
    st.stop()

result_files = list(results_dir.rglob("*_result.xlsx"))
if not result_files:
    st.error("No result files found in the results directory.")
    st.stop()

file_options = {f.parent.name + " / " + f.name: f for f in result_files}
selected_file_key = st.sidebar.selectbox("Select Run", list(file_options.keys()))
selected_file = file_options[selected_file_key]

st.sidebar.markdown("---")

@st.cache_data
def load_data(file_path, mtime):
    try:
        df_rankings = pd.read_excel(file_path, sheet_name='Rankings')
    except:
        df_rankings = pd.DataFrame()
        
    try:
        df_dist = pd.read_excel(file_path, sheet_name='Dist_RawData')
    except:
        df_dist = pd.DataFrame()
        
    return df_rankings, df_dist

mtime = os.path.getmtime(selected_file)
df_rankings, df_dist = load_data(selected_file, mtime)

if df_dist.empty:
    st.warning("No distribution data available for this run.")
    st.stop()

# --- Session State Initialization ---
if 'drill_path' not in st.session_state:
    st.session_state.drill_path = []
if 'top_x_val' not in st.session_state:
    st.session_state.top_x_val = 10
if 'threshold_val' not in st.session_state:
    st.session_state.threshold_val = 1.0

# Rankings Overview
if not df_rankings.empty:
    st.header("Pallet Rankings Overview")
    df_rankings['Label'] = df_rankings['File'] + " - " + df_rankings['Tab']
    
    # Format annotations
    df_rankings['Annotation'] = df_rankings.apply(
        lambda r: f"ROI: {r['Pallet ROI (%)']:.2f}%<br>Profit: ${r['Estimated Pallet Profit']:,.0f}<br>Cost: ${r['Total Pallet Cost']:,.0f}<br>API Hit Rate*: {r.get('API Hit Rate by Cost', 0.0)*100:.2f}%", axis=1
    )
    
    fig_profit = px.bar(
        df_rankings, 
        x='Label', 
        y='Pallet ROI (%)', 
        color='Estimated Pallet Profit',
        title='Overview by Pallet',
        text='Annotation',
        color_continuous_scale='Viridis',
        custom_data=['Estimated Pallet Profit', 'Total Pallet Cost', 'Realized Quantity']
    )
    
    fig_profit.update_traces(
        textposition='inside', 
        textangle=0,
        hovertemplate="<b>%{x}</b><br>ROI: %{y:.2f}%<br>Profit: $%{customdata[0]:,.0f}<br>Cost: $%{customdata[1]:,.0f}<br>Quantity: %{customdata[2]}<extra></extra>"
    )
    
    fig_profit.update_layout(
        yaxis_title="ROI (%)", 
        xaxis_title="",
        hoverlabel=dict(bgcolor='#FFFFE0'),
        coloraxis_colorbar=dict(
            title="Profit",
            dtick=50000,
            tickformat="$,.0f"
        )
    )
    st.plotly_chart(fig_profit, use_container_width=True)
    st.caption("* API Hit Rate* represents the percentage of the total item cost that was successfully enriched with Keepa API data.")

st.markdown("---")
st.header("Hierarchical Category Analysis")

# --- Sidebar Controls ---
st.sidebar.header("Category Filters")
tabs = ['All'] + sorted(df_dist['Tab'].unique().tolist())
selected_tab = st.sidebar.selectbox("Filter by Detail (Tab)", tabs)

st.sidebar.markdown("---")
st.sidebar.subheader("Visualization Metric")
dist_metric = st.sidebar.radio("Analyze Distribution By:", ['Profit Contribution (%)', 'ROI (%)'])

st.sidebar.markdown("---")
st.sidebar.subheader("Grouping Limits")
# Toggle for the limit mode. The inactive input gets disabled (grayed out).
limit_mode = st.sidebar.radio("Limit Categories By:", ['Threshold (%)', 'Top X'])

if limit_mode == 'Threshold (%)':
    st.session_state.threshold_val = st.sidebar.number_input("Group items under X% into 'Other'", min_value=0.0, max_value=100.0, value=st.session_state.threshold_val, step=0.5)
    st.sidebar.number_input("Show Top X categories", value=st.session_state.top_x_val, disabled=True)
else:
    st.sidebar.number_input("Group items under X% into 'Other'", value=st.session_state.threshold_val, disabled=True)
    st.session_state.top_x_val = st.sidebar.number_input("Show Top X categories", min_value=1, max_value=100, value=st.session_state.top_x_val, step=1)

# --- Drill Down Navigation UI ---
col_nav1, col_nav2, _ = st.columns([2, 2, 8])
with col_nav1:
    if st.button("⬅️ Go Back One Layer", disabled=len(st.session_state.drill_path) == 0):
        st.session_state.drill_path.pop()
        st.rerun()
with col_nav2:
    if st.button("⏮️ Back to Root", disabled=len(st.session_state.drill_path) == 0):
        st.session_state.drill_path = []
        st.rerun()

path_str = " > ".join(st.session_state.drill_path) if st.session_state.drill_path else "Root"
st.markdown(f"**Current Drill Path:** `{path_str}`")
st.caption("💡 *Click on any category bar in the chart below to drill down into its subcategories.*")

# --- Data Processing ---
df_subset = df_dist.copy()

# 1. Filter by Tab
if selected_tab != 'All':
    df_subset = df_subset[df_subset['Tab'] == selected_tab]

# Calculate Global Totals for the selected tab(s)
global_total_profit = df_subset['Estimated_Profit'].sum()

# 2. Filter by Drill Path (Only keep items belonging to the current clicked tree node)
if st.session_state.drill_path:
    prefix = " > ".join(st.session_state.drill_path)
    df_subset = df_subset[df_subset['CategoryTree'].str.startswith(prefix, na=False)]

current_depth = len(st.session_state.drill_path) + 1

def get_category_at_depth(tree_str, target_depth):
    if pd.isna(tree_str) or str(tree_str).strip() == '':
        return "Unknown"
    tree_str = str(tree_str).strip()
    parts = [p.strip() for p in tree_str.split(">")]
    if len(parts) >= target_depth:
        return " > ".join(parts[:target_depth])
    else:
        return " > ".join(parts)

df_subset['DynamicCategory'] = df_subset['CategoryTree'].apply(lambda x: get_category_at_depth(x, current_depth))

# 3. Sidebar Filter for the *current* depth
all_dynamic_cats = sorted(df_subset['DynamicCategory'].unique().tolist())
selected_cats = st.sidebar.multiselect(f"Filter at Depth {current_depth}", all_dynamic_cats, default=[])
if selected_cats:
    df_subset = df_subset[df_subset['DynamicCategory'].isin(selected_cats)]

# --- Plotting ---
df_cat = df_subset.groupby('DynamicCategory', as_index=False).agg({
    'Estimated_Profit': 'sum', 'Cost': 'sum', 'Quantity': 'sum'
})

# We keep categories that have cost or profit to allow ROI calculation.
# If filtering by Profit, we might only want positive profit.
if dist_metric == 'Profit Contribution (%)':
    df_cat = df_cat[df_cat['Estimated_Profit'] > 0].copy()

local_total_profit = df_cat['Estimated_Profit'].sum()

if not df_cat.empty and (local_total_profit > 0 or dist_metric == 'ROI (%)'):
    # Calculations
    df_cat['Percentage'] = (df_cat['Estimated_Profit'] / local_total_profit * 100) if local_total_profit > 0 else 0
    df_cat['Global_Percentage'] = (df_cat['Estimated_Profit'] / global_total_profit * 100) if global_total_profit > 0 else 0
    df_cat['ROI_Pct'] = (df_cat['Estimated_Profit'] / df_cat['Cost'] * 100).fillna(0)
    
    # Sort order depends on metric
    if dist_metric == 'Profit Contribution (%)':
        df_cat = df_cat.sort_values('Percentage', ascending=False)
        sort_col = 'Percentage'
    else:
        df_cat = df_cat.sort_values('ROI_Pct', ascending=False)
        sort_col = 'ROI_Pct'
        
    # Apply Grouping Limits (Grouping logic is typically based on Profit Contribution, even if plotting ROI)
    # However, if we group by ROI, "Other" ROI is sum(profit)/sum(cost).
    if limit_mode == 'Threshold (%)':
        mask = df_cat['Percentage'] >= st.session_state.threshold_val
        other_label = f"Other (<{st.session_state.threshold_val}% Profit)"
    else:
        mask = [True] * min(st.session_state.top_x_val, len(df_cat)) + [False] * max(0, len(df_cat) - st.session_state.top_x_val)
        df_cat['is_top_x'] = mask
        mask = df_cat['is_top_x']
        other_label = f"Other (Not in Top {st.session_state.top_x_val})"

    df_main = df_cat[mask].copy()
    df_other = df_cat[~mask]

    if not df_other.empty:
        other_profit = df_other['Estimated_Profit'].sum()
        other_cost = df_other['Cost'].sum()
        other_row = pd.DataFrame([{
            'DynamicCategory': other_label,
            'Estimated_Profit': other_profit,
            'Cost': other_cost,
            'Quantity': df_other['Quantity'].sum(),
            'Percentage': df_other['Percentage'].sum(),
            'Global_Percentage': df_other['Global_Percentage'].sum(),
            'ROI_Pct': (other_profit / other_cost * 100) if other_cost > 0 else 0
        }])
        df_main = pd.concat([df_main, other_row], ignore_index=True)
        
    df_main = df_main.sort_values(sort_col, ascending=True)
    
    # Format annotations
    def make_annotation(r):
        local_pct = f"{r['Percentage']:.2f}%"
        global_pct = f"{r['Global_Percentage']:.2f}% global"
        roi = f"ROI: {r['ROI_Pct']:.2f}%"
        profit = f"Profit: ${r['Estimated_Profit']:,.0f}"
        cost = f"Cost: ${r['Cost']:,.0f}"
        qty = f"Qty: {int(r['Quantity'])}"
        
        if len(st.session_state.drill_path) > 0:
            return f"{local_pct} ({global_pct}) | {roi} | {profit} | {cost} | {qty}"
        else:
            return f"{local_pct} | {roi} | {profit} | {cost} | {qty}"

    df_main['Annotation'] = df_main.apply(make_annotation, axis=1)
    
    plot_x = 'Percentage' if dist_metric == 'Profit Contribution (%)' else 'ROI_Pct'
    x_title = 'Percentage of Local Profit (%)' if dist_metric == 'Profit Contribution (%)' else 'ROI (%)'
    
    fig_cat = px.bar(
        df_main, y='DynamicCategory', x=plot_x, text='Annotation', orientation='h',
        title=f"Category Distribution at Depth {current_depth}"
    )
    fig_cat.update_traces(textposition='auto')
    fig_cat.update_layout(
        xaxis_title=x_title, yaxis_title='Category Path',
        height=max(400, len(df_main) * 40)
    )
    
    # Render chart and capture click events
    selection = st.plotly_chart(fig_cat, use_container_width=True, on_select="rerun", selection_mode="points")
    
    # Handle click event for drill down
    if selection and selection.get("selection") and selection["selection"].get("points"):
        point = selection["selection"]["points"][0]
        clicked_cat = point.get("y")
        
        # Prevent drilling down into the 'Other' bucket or 'Unknown'
        if clicked_cat and not clicked_cat.startswith("Other") and clicked_cat != "Unknown":
            new_path = [p.strip() for p in clicked_cat.split(">")]
            # Only rerun if it's actually a deeper path
            if new_path != st.session_state.drill_path:
                st.session_state.drill_path = new_path
                st.rerun()
else:
    st.info("No data available for the selected filters.")

# Price Distribution
if 'Original_MSRP' in df_subset.columns:
    st.markdown("---")
    st.header("Price Distribution")
    
    col_price1, col_price2 = st.columns([1, 2])
    with col_price1:
        price_metric = st.radio("Price Metric:", ['Original MSRP', 'Estimated Sale Price'])
    with col_price2:
        bin_method = st.radio("Binning Method:", ["Evenly Spaced", "Quantiles (Equal Data Points)", "Custom Boundaries"])
        
    price_col = 'Original_MSRP' if price_metric == 'Original MSRP' else 'Estimated_Value'
    
    if bin_method in ["Evenly Spaced", "Quantiles (Equal Data Points)"]:
        num_bins = st.slider("Number of Bins", min_value=2, max_value=20, value=5)
        if bin_method == "Evenly Spaced":
            df_subset['Dynamic_Bin'] = pd.cut(df_subset[price_col], bins=num_bins, precision=0)
        else:
            try:
                df_subset['Dynamic_Bin'] = pd.qcut(df_subset[price_col], q=num_bins, duplicates='drop', precision=0)
            except Exception:
                df_subset['Dynamic_Bin'] = pd.cut(df_subset[price_col], bins=num_bins, precision=0)
    else:
        st.subheader("Custom Boundaries")
        if 'custom_bounds' not in st.session_state:
            st.session_state.custom_bounds = [0.0, 20.0, 50.0, 100.0, 200.0, 5000.0]
            
        b_col1, b_col2 = st.columns([1, 4])
        with b_col1:
            if st.button("➕ Add Bound"):
                st.session_state.custom_bounds.append(st.session_state.custom_bounds[-1] + 50.0)
                st.rerun()
            if st.button("➖ Remove Bound") and len(st.session_state.custom_bounds) > 2:
                st.session_state.custom_bounds.pop()
                st.rerun()
        with b_col2:
            cols = st.columns(len(st.session_state.custom_bounds))
            for i in range(len(st.session_state.custom_bounds)):
                st.session_state.custom_bounds[i] = cols[i].number_input(f"Bound {i}", value=float(st.session_state.custom_bounds[i]), step=10.0, key=f"bound_{i}")
        
        st.session_state.custom_bounds = sorted(list(set(st.session_state.custom_bounds)))
        try:
            df_subset['Dynamic_Bin'] = pd.cut(df_subset[price_col], bins=st.session_state.custom_bounds, precision=0)
        except Exception as e:
            st.error(f"Error creating custom bins: {e}")
            df_subset['Dynamic_Bin'] = 'Error'
            
    df_subset['Dynamic_Bin'] = df_subset['Dynamic_Bin'].astype(str)
    df_subset_msrp = df_subset[df_subset['Dynamic_Bin'] != 'nan']
    df_subset_msrp = df_subset_msrp[df_subset_msrp['Dynamic_Bin'] != 'Error']
    
    if not df_subset_msrp.empty:
        df_msrp = df_subset_msrp.groupby('Dynamic_Bin', as_index=False).agg({'Estimated_Profit': 'sum', 'Cost': 'sum'})
        
        local_total_profit_msrp = df_msrp['Estimated_Profit'].sum()
        df_msrp['Percentage'] = (df_msrp['Estimated_Profit'] / local_total_profit_msrp * 100) if local_total_profit_msrp > 0 else 0
        df_msrp['Global_Percentage'] = (df_msrp['Estimated_Profit'] / global_total_profit * 100) if global_total_profit > 0 else 0
        
        def extract_lower_bound(bin_str):
            try:
                return float(bin_str.split(',')[0].strip('(['))
            except:
                return 0.0
                
        df_msrp['sort_key'] = df_msrp['Dynamic_Bin'].apply(extract_lower_bound)
        df_msrp = df_msrp.sort_values('sort_key')
        
        df_msrp['ROI_Pct'] = (df_msrp['Estimated_Profit'] / df_msrp['Cost'] * 100).fillna(0)
        
        def make_msrp_annotation(r):
            local_pct = f"{r['Percentage']:.2f}%"
            global_pct = f"{r['Global_Percentage']:.2f}% global"
            roi = f"ROI: {r['ROI_Pct']:.2f}%"
            profit = f"Profit: ${r['Estimated_Profit']:,.0f}"
            cost = f"Cost: ${r['Cost']:,.0f}"
            
            if len(st.session_state.drill_path) > 0:
                return f"{local_pct} ({global_pct})<br>{roi}<br>{profit}<br>{cost}"
            else:
                return f"{local_pct}<br>{roi}<br>{profit}<br>{cost}"
                
        df_msrp['Annotation'] = df_msrp.apply(make_msrp_annotation, axis=1)
        
        if dist_metric == 'ROI (%)':
            fig_msrp = px.bar(df_msrp, x='Dynamic_Bin', y='ROI_Pct', text='Annotation', title=f'ROI by {price_metric} Range')
            fig_msrp.update_layout(yaxis_title="ROI (%)", xaxis_title=f"{price_metric} Range ($)")
        else:
            fig_msrp = px.bar(df_msrp, x='Dynamic_Bin', y='Percentage', text='Annotation', title=f'Profit Contribution by {price_metric} Range')
            fig_msrp.update_layout(yaxis_title="Percentage of Local Profit (%)", xaxis_title=f"{price_metric} Range ($)")
            
        fig_msrp.update_traces(textposition='inside', textangle=0)
        fig_msrp.update_layout(hoverlabel=dict(bgcolor='#FFFFE0'))
        st.plotly_chart(fig_msrp, use_container_width=True)
    else:
        st.info("No data available for the selected price metric and bins.")
