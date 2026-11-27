package subjects

import (
	"fmt"
)

const (
	// DefaultConfigurePattern is the standard configure routing pattern.
	DefaultConfigurePattern = "cmd.configure.%s"
	// DefaultActionPattern is the standard action routing pattern.
	DefaultActionPattern = "cmd.action.%s.%s"
	// DefaultResultPattern is the standard result routing pattern.
	DefaultResultPattern = "result.%s"
	// DefaultStatusPattern is the standard status routing pattern.
	DefaultStatusPattern = "status.%s"
	// DefaultHealthPattern is the standard health routing pattern.
	DefaultHealthPattern = "health.%s"
	// DefaultStatePattern is the standard state routing pattern.
	DefaultStatePattern = "state.%s"
	// DefaultStateGetPattern is the standard state get routing pattern.
	DefaultStateGetPattern = "state.get.%s"
)

// Patterns contains all subject patterns used by routing helpers.
type Patterns struct {
	ConfigurePattern string
	ActionPattern    string
	ResultPattern    string
	StatusPattern    string
	HealthPattern    string
	StatePattern     string
	StateGetPattern  string
}

// Config contains optional subject-pattern overrides.
type Config struct {
	ConfigurePattern string
	ActionPattern    string
	ResultPattern    string
	StatusPattern    string
	HealthPattern    string
	StatePattern     string
	StateGetPattern  string
}

// DefaultPatterns returns the default routing patterns.
func DefaultPatterns() Patterns {
	return Patterns{
		ConfigurePattern: DefaultConfigurePattern,
		ActionPattern:    DefaultActionPattern,
		ResultPattern:    DefaultResultPattern,
		StatusPattern:    DefaultStatusPattern,
		HealthPattern:    DefaultHealthPattern,
		StatePattern:     DefaultStatePattern,
		StateGetPattern:  DefaultStateGetPattern,
	}
}

// PatternsFromConfig resolves configured patterns while preserving defaults.
func PatternsFromConfig(cfg Config) (Patterns, error) {
	p := DefaultPatterns()

	if cfg.ConfigurePattern != "" {
		p.ConfigurePattern = cfg.ConfigurePattern
	}
	if cfg.ActionPattern != "" {
		p.ActionPattern = cfg.ActionPattern
	}
	if cfg.ResultPattern != "" {
		p.ResultPattern = cfg.ResultPattern
	}
	if cfg.StatusPattern != "" {
		p.StatusPattern = cfg.StatusPattern
	}
	if cfg.HealthPattern != "" {
		p.HealthPattern = cfg.HealthPattern
	}
	if cfg.StatePattern != "" {
		p.StatePattern = cfg.StatePattern
	}
	if cfg.StateGetPattern != "" {
		p.StateGetPattern = cfg.StateGetPattern
	}

	if err := validatePattern("configure_pattern", p.ConfigurePattern, 1); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("action_pattern", p.ActionPattern, 2); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("result_pattern", p.ResultPattern, 1); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("status_pattern", p.StatusPattern, 1); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("health_pattern", p.HealthPattern, 1); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("state_pattern", p.StatePattern, 1); err != nil {
		return Patterns{}, err
	}
	if err := validatePattern("state_get_pattern", p.StateGetPattern, 1); err != nil {
		return Patterns{}, err
	}

	return p, nil
}

// Builder centralizes subject generation for publish and subscribe paths.
type Builder struct {
	patterns Patterns
}

// NewBuilder creates a Builder with validated patterns.
func NewBuilder(patterns Patterns) (*Builder, error) {
	if err := validatePattern("configure_pattern", patterns.ConfigurePattern, 1); err != nil {
		return nil, err
	}
	if err := validatePattern("action_pattern", patterns.ActionPattern, 2); err != nil {
		return nil, err
	}
	if err := validatePattern("result_pattern", patterns.ResultPattern, 1); err != nil {
		return nil, err
	}
	if err := validatePattern("status_pattern", patterns.StatusPattern, 1); err != nil {
		return nil, err
	}
	if err := validatePattern("health_pattern", patterns.HealthPattern, 1); err != nil {
		return nil, err
	}
	if err := validatePattern("state_pattern", patterns.StatePattern, 1); err != nil {
		return nil, err
	}
	if err := validatePattern("state_get_pattern", patterns.StateGetPattern, 1); err != nil {
		return nil, err
	}

	return &Builder{patterns: patterns}, nil
}

// NewDefaultBuilder creates a Builder using default patterns.
func NewDefaultBuilder() *Builder {
	builder, _ := NewBuilder(DefaultPatterns())
	return builder
}

// ConfigureSubject builds the configure subject for a target.
func (b *Builder) ConfigureSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.ConfigurePattern, target), nil
}

// ActionSubject builds the action subject for a target and action.
func (b *Builder) ActionSubject(target, action string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	if err := ValidateAction(action); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.ActionPattern, target, action), nil
}

// ResultSubject builds the result subject for a target.
func (b *Builder) ResultSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.ResultPattern, target), nil
}

// StatusSubject builds the status subject for a target.
func (b *Builder) StatusSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.StatusPattern, target), nil
}

// HealthSubject builds the health subject for a target.
func (b *Builder) HealthSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.HealthPattern, target), nil
}

// StateSubject builds the periodic state subject for a target.
func (b *Builder) StateSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.StatePattern, target), nil
}

// StateGetSubject builds the on-demand state get subject for a target.
func (b *Builder) StateGetSubject(target string) (string, error) {
	if err := ValidateTarget(target); err != nil {
		return "", err
	}
	return fmt.Sprintf(b.patterns.StateGetPattern, target), nil
}
