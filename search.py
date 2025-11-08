from vocabulary import word_index

while True:
    try:
        word = input("Word:\n>> ").lower()
    except KeyboardInterrupt:
        print()
        exit()

    if word in word_index:
        print(f"Zack D. Films has said this word {len(word_index[word])} times!")
    else:
        print("Zack D. Films has said this word 0 times!")