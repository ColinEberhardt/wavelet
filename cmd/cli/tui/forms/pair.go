package forms

import "strconv"

// Setter is the function called when the user submits the form. This function
// should set the fields of the structs, doing type conversion if needed.
//
// If an error is returned and is not nil, the form will prompt the user to
// input something else.
type Setter func(output string) error

// Pair is the struct for each form field
type Pair struct {
	Name string
	// if error is not nil, pop up an error dialog and reprompt
	Value Setter

	// If false, invalid
	Validator Validator

	Completer Completer
}

// NewPair creates a new Pair
func NewPair(name string, value Setter) Pair {
	return Pair{name, value, nil, nil}
}

// StringPair creates a new Pair from a string pointer.
func StringPair(name string, value *string) Pair {
	return Pair{name, func(output string) error {
		*value = output
		return nil
	}, nil, nil}
}

func IntPair(name string, value *int) Pair {
	return Pair{name, func(output string) error {
		i, err := strconv.Atoi(output)
		if err != nil {
			return err
		}

		*value = i
		return nil
	}, nil, nil}
}