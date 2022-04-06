package broker

import "testing"

func TestVariableLengthEncoding(t *testing.T) {
	t.Parallel()

	l := 0
	ve := VariableLengthEncode([]byte{}, l)
	if len(ve) != 1 || ve[0] != 0 {
		t.Fatal(l)
	}

	l = 127
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 1 || ve[0] != 127 {
		t.Fatal(l)
	}

	l = 128
	e := []byte{0x80, 0x01, 0, 0}
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 2 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}

	l = 16383
	e[0], e[1] = 0xFF, 0x7F
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 2 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}

	l = 16384
	e[0], e[1], e[2] = 0x80, 0x80, 0x01
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 3 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}

	l = 2097151
	e[0], e[1], e[2] = 0xFF, 0xFF, 0x7F
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 3 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}

	l = 2097152
	e[0], e[1], e[2], e[3] = 0x80, 0x80, 0x80, 0x01
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 4 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}

	l = 268435455
	e[0], e[1], e[2], e[3] = 0xFF, 0xFF, 0xFF, 0x7F
	ve = VariableLengthEncode(ve[:0], l)
	if len(ve) != 4 {
		t.Fatal(l)
	}
	for i, b := range ve {
		if b != e[i] {
			t.Fatal(l)
		}
	}
}

func TestUTF8(t *testing.T) {
	t.Parallel()

	// U+0000 invalid
	if err := checkUTF8([]byte{0x00}, false); err == nil {
		t.Fatal(0)
	}

	// U+D7FF valid
	if err := checkUTF8([]byte{0xED, 0x9F, 0xBF, 0x31}, false); err != nil {
		t.Fatal(1, err)
	}

	// U+D800 invalid
	if err := checkUTF8([]byte{0xED, 0xA0, 0x80}, false); err == nil {
		t.Fatal(3)
	}

	// U+DFFF invalid
	if err := checkUTF8([]byte{0xED, 0xBF, 0xBF}, false); err == nil {
		t.Fatal(4)
	}

	// U+E000 valid
	if err := checkUTF8([]byte{0xEE, 0x80, 0x80}, false); err != nil {
		t.Fatal(5, err)
	}

	// U+0001, U+FEFF valid
	if err := checkUTF8([]byte{0x01, 0xEF, 0xBB, 0xBF, 0x59}, false); err != nil {
		t.Fatal(6, err)
	}

	// U+0001, U+FEFF, U+0000 invalid
	if err := checkUTF8([]byte{0x01, 0xEF, 0xBB, 0xBF, 0x59, 0}, false); err == nil {
		t.Fatal(6)
	}
}
