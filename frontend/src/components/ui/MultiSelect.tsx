import { useEffect, useId, useMemo, useRef, useState } from 'react';

export function MultiSelect({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: string[];
  value: string[];
  onChange: (value: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const controlId = useId();
  const containerRef = useRef<HTMLDivElement>(null);
  const selectedSet = useMemo(() => new Set(value), [value]);
  const filteredOptions = useMemo(
    () => options.filter((option) => option.toLowerCase().includes(query.trim().toLowerCase())),
    [options, query],
  );

  function toggleOption(option: string) {
    if (selectedSet.has(option)) {
      onChange(value.filter((item) => item !== option));
      return;
    }

    onChange([...value, option]);
  }

  useEffect(() => {
    if (!open) return;

    function handleOutsideInteraction(event: PointerEvent) {
      if (!containerRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    document.addEventListener('pointerdown', handleOutsideInteraction);
    return () => document.removeEventListener('pointerdown', handleOutsideInteraction);
  }, [open]);

  return (
    <div className="multi-select" ref={containerRef}>
      <div className="multi-select-label" id={`${controlId}-label`}>
        {label}
      </div>
      <button
        aria-expanded={open}
        aria-labelledby={`${controlId}-label`}
        className="multi-select-trigger"
        type="button"
        onClick={() => setOpen((current) => !current)}
      >
        <span>{value.length ? `${value.length} selecionado${value.length > 1 ? 's' : ''}` : 'Todos'}</span>
        <strong>{open ? 'Fechar' : 'Selecionar'}</strong>
      </button>

      {value.length > 0 ? (
        <div className="selected-chips" aria-label={`${label} selecionados`}>
          {value.slice(0, 5).map((item) => (
            <button key={item} type="button" onClick={() => toggleOption(item)}>
              {item}
              <span aria-hidden="true">x</span>
            </button>
          ))}
          {value.length > 5 ? <em>+{value.length - 5}</em> : null}
          <button className="clear-chip" type="button" onClick={() => onChange([])}>
            Limpar
          </button>
        </div>
      ) : (
        <small>Sem selecao aplica todos.</small>
      )}

      {open ? (
        <div className="multi-select-menu">
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={`Buscar ${label.toLowerCase()}`}
          />
          <div className="multi-select-actions">
            <button type="button" onClick={() => onChange(filteredOptions)}>
              Selecionar visiveis
            </button>
            <button type="button" onClick={() => onChange([])}>
              Limpar
            </button>
          </div>
          <div className="multi-select-options">
            {filteredOptions.map((option) => (
              <button
                className={selectedSet.has(option) ? 'selected' : ''}
                key={option}
                type="button"
                onClick={() => toggleOption(option)}
              >
                <span>{option}</span>
                {selectedSet.has(option) ? <strong>Selecionado</strong> : null}
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
