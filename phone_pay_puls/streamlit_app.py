import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt


engine = create_engine("mysql+pymysql://root:PraveenK12345@localhost:3306/phonepay_puls")


Agg_Trans = pd.read_sql("SELECT * FROM agg_trans", con=engine)
agg_insurance = pd.read_sql("SELECT * FROM agg_insurance", con=engine)
agg_user = pd.read_sql("SELECT * FROM agg_user", con=engine)
map_trans = pd.read_sql("SELECT * FROM map_Trans", con=engine)
map_insurance = pd.read_sql("SELECT * FROM map_Insurance", con=engine)
map_user = pd.read_sql("SELECT * FROM map_User", con=engine)
top_trans = pd.read_sql("SELECT * FROM top_trans", con=engine)
top_user = pd.read_sql("SELECT * FROM top_user", con=engine)
top_insurance = pd.read_sql("SELECT * FROM top_insurance", con=engine)


st.sidebar.header("📊 Filter Options")

state_mode = st.sidebar.radio("Filter by State", ["All States", "Statewise"])
year_mode = st.sidebar.radio("Filter by Year", ["All Years", "Yearwise"])

state_filter = None
year_filter = None

if state_mode == "Statewise":
    States = sorted(Agg_Trans["State"].unique().tolist())
    state_filter = st.sidebar.selectbox("Select State", States)

if year_mode == "Yearwise":
    Years = sorted(Agg_Trans["Year"].unique().tolist())
    year_filter = st.sidebar.selectbox("Select Year", Years)


filtered_data = Agg_Trans.copy()
if state_filter:
    filtered_data = filtered_data[filtered_data["State"] == state_filter]
if year_filter:
    filtered_data = filtered_data[filtered_data["Year"] == year_filter]


st.title("📱 PhonePe Transaction Insights Dashboard")
st.markdown("### Explore Trends Across States and Years")


st.header("🧠 Case Studies & Insights")

case_options = [
    "Select a Case Study",
    "Insurance Transactions Analysis",
    "Insurance Penetration and Growth Potential Analysis",
    "Transaction Analysis for Market Expansion",
    "Decoding Transaction Dynamics on PhonePe",
    "User Engagement and Growth Strategy"
]
selected_case = st.selectbox("Choose a Case Study", case_options)


if selected_case == "Insurance Transactions Analysis":
    st.subheader("💡 Insurance Transactions Analysis by State")
    if "Insurance_amount" in agg_insurance.columns:
        ins_data = agg_insurance.groupby("State")["Insurance_amount"].sum().reset_index()
        top10 = ins_data.nlargest(10, "Insurance_amount")

        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(ins_data)
        with col2:
            st.write("### 🏆 Top 10 States by Insurance Amount")
            st.dataframe(top10)

        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(data=ins_data, x="State", y="Insurance_amount", palette="viridis", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

        
        st.write("### 🔝 Top 10 States - Insurance Amount")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=top10, x="State", y="Insurance_amount", palette="magma", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

    else:
        st.warning("No 'Insurance_amount' column found in the data.")

elif selected_case == "Insurance Penetration and Growth Potential Analysis":
    st.subheader("📊 Insurance Penetration Analysis")
    if "Insurance_count" in agg_insurance.columns:
        penetration = agg_insurance.groupby("State")["Insurance_count"].sum().reset_index()
        top10 = penetration.nlargest(10, "Insurance_count")

        st.dataframe(penetration)
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(data=penetration, x="State", y="Insurance_count", palette="coolwarm", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

        st.write("### 🔝 Top 10 States - Insurance Count")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=top10, x="State", y="Insurance_count", palette="plasma", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
    else:
        st.warning("No 'Insurance_count' column found in the data.")

elif selected_case == "Transaction Analysis for Market Expansion":
    st.subheader("📈 Transaction vs Insurance Amount by State")

    if "Transacion_amount" in Agg_Trans.columns and "Insurance_amount" in agg_insurance.columns:
        trans_state = Agg_Trans.groupby("State")["Transacion_amount"].sum().reset_index()
        ins_state = agg_insurance.groupby("State")["Insurance_amount"].sum().reset_index()
        merged = pd.merge(trans_state, ins_state, on="State", how="inner").sort_values("Transacion_amount", ascending=False)
        merged["Difference"] = merged["Transacion_amount"] - merged["Insurance_amount"]
        top10 = merged.nlargest(10, "Transacion_amount")

        st.dataframe(merged)

        
        fig, ax = plt.subplots(figsize=(14, 6))
        width = 0.4
        x = range(len(merged))
        ax.bar(x, merged["Transacion_amount"], width, label="Transactions", color="skyblue")
        ax.bar(x, merged["Insurance_amount"], width, label="Insurance", color="orange", bottom=None)
        plt.xticks(x, merged["State"], rotation=45)
        plt.title("Transactions vs Insurance Amount by State", fontsize=13, fontweight='bold')
        plt.ylabel("Amount (₹)")
        plt.legend()
        plt.tight_layout()
        st.pyplot(fig)

        
        st.write("### 🔝 Top 10 States - Transaction Amount")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=top10, x="State", y="Transacion_amount", palette="crest", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

    else:
        st.warning("Required columns for this analysis are missing. Check table names or column spellings.")

elif selected_case == "Decoding Transaction Dynamics on PhonePe":
    st.subheader("📊 Transaction Distribution by State")
    txn_data = filtered_data.groupby("State")["Transacion_amount"].sum().reset_index()
    top10 = txn_data.nlargest(10, "Transacion_amount")

    st.dataframe(txn_data)

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(data=txn_data, x="State", y="Transacion_amount", palette="coolwarm", ax=ax)
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)

    st.write("### 🔝 Top 10 States - Transaction Amount")
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=top10, x="State", y="Transacion_amount", palette="rocket", ax=ax)
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(fig)

elif selected_case == "User Engagement and Growth Strategy":
    st.subheader("🧠 User Engagement by Brand")
    if "User_count" in agg_user.columns and "User_brand" in agg_user.columns:
        user_data = agg_user.groupby("User_brand")["User_count"].sum().reset_index()
        top10 = user_data.nlargest(10, "User_count")

        st.dataframe(user_data)

        fig, ax = plt.subplots(figsize=(12, 6))
        sns.barplot(data=user_data, x="User_brand", y="User_count", palette="viridis", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)

        st.write("### 🔝 Top 10 Brands - User Engagement")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=top10, x="User_brand", y="User_count", palette="flare", ax=ax)
        plt.xticks(rotation=45)
        plt.tight_layout()
        st.pyplot(fig)
    else:
        st.warning("Required columns for user analysis are missing.")

else:
    st.info(" Select a case study from the dropdown above to explore insights.")
