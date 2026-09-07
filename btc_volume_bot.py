async def main():
    try:
        print("1. Fetching data...")
        df = await fetch_klines(SYMBOL, limit=LOOKBACK * 2 + 50)
        print(f"2. Data fetched: {len(df)} candles")

        print("3. Creating chart...")
        photo = create_chart(df)
        print(f"4. Chart created: {len(photo)} bytes")

        caption = (f"{SYMBOL} 1m Chart\n"
                   f"Lookback: {LOOKBACK}\n"
                   f"Std Multiplier: {STD_MULT}σ")

        print("5. Sending to Telegram...")
        await send_photo(photo, caption)
        print("6. Done. Exiting.")

    except Exception as e:
        print("ERROR:", str(e))
        import traceback
        traceback.print_exc()