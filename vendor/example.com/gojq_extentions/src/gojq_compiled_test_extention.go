package gojq_test_extention

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	lhm "github.com/xboshy/linkedhashmap"
)

/*
 * linkedhashmap structs
 */
type lhm_map_functions struct {
}

func (mf *lhm_map_functions) ExpiredHandler(key *string, value **regexp.Regexp) {
}

func (mf *lhm_map_functions) CapacityRule(curcapacity uint64, curlen uint64, head **regexp.Regexp, tail **regexp.Regexp) uint64 {
	return curcapacity
}

/*
 * compiled_regex struct
 */
type compiled_regex struct {
	regex  *lhm.Map[string, *regexp.Regexp]
	rwlock sync.RWMutex
}

/*
 * Global variable initialization
 */
var lhm_mf lhm.MapFunctions[string, *regexp.Regexp] = &lhm_map_functions{}

var regex_map *compiled_regex = &compiled_regex{
	regex: lhm.New(10000, lhm_mf),
}

/*
 * private functions
 */
func compile_regexp(re string) (*regexp.Regexp, error) {
	re = strings.ReplaceAll(re, "(?<", "(?P<")
	r, err := regexp.Compile(re)
	if err != nil {
		return nil, fmt.Errorf("compile_regexp - invalid regular expression %q: %s", re, err)
	}
	return r, nil
}

func (cr *compiled_regex) get(re string) (*regexp.Regexp, error) {
	cr.rwlock.RLock()
	cre := cr.regex.Get(re)
	cr.rwlock.RUnlock()
	if cre == nil {
		cr.rwlock.Lock()
		defer cr.rwlock.Unlock()

		cre = cr.regex.Get(re)
		if cre == nil {
			ncre, err := compile_regexp(re)
			if err != nil {
				return nil, err
			}
			cr.regex.Push(re, ncre)
			cre = &ncre
		}
	}
	return *cre, nil
}

/*
 * Exported functions
 */
func Compiled_test(in any, args []any) any {
	re := args[0]

	s, ok := in.(string)
	if !ok {
		return fmt.Errorf("compile_test - input is not a string %q", in)
	}
	restr, ok := re.(string)
	if !ok {
		return fmt.Errorf("compile_test - regex is not a string %q", re)
	}

	r, err := regex_map.get(restr)
	if err != nil {
		return err
	}

	got := r.FindStringSubmatchIndex(s)
	return got != nil
}
