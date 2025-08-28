import React, { useEffect, useState } from 'react';
import {
  TextField,
  Button,
  CircularProgress,
  Paper,
  Grid,
  LinearProgress,
  List,
  ListItem,
  ListItemText,
  Divider,
  Typography,
} from '@material-ui/core';
import { useDispatch, useSelector } from 'react-redux';
import * as Actions from '../store/actions';
import { withRouter } from 'react-router-dom';
import { useLazyQuery, useMutation } from '@apollo/react-hooks';
import {
  FactsMngSharkAttack as GqlGet,
  FactsMngCreateSharkAttack as GqlCreate,
  FactsMngUpdateSharkAttack as GqlUpdate,
} from '../gql/SharkAttack';

const FIELD_LIST = [
  ['date', 'Fecha'],
  ['year', 'Año'],
  ['type', 'Tipo'],
  ['country', 'País'],
  ['area', 'Área'],
  ['location', 'Ubicación'],
  ['activity', 'Actividad'],
  ['name', 'Nombre'],
  ['sex', 'Sexo'],
  ['age', 'Edad'],
  ['injury', 'Lesión'],
  ['fatal_y_n', 'Fatal (S/N)'],
  ['time', 'Hora'],
  ['species', 'Especie'],
  ['investigator_or_source', 'Investigador / Fuente'],
  ['pdf', 'PDF'],
  ['href_formula', 'Fórmula Enlace'],
  ['href', 'Enlace'],
  ['case_number', 'Número de Caso'],
  ['case_number0', 'Número de Caso (Alt)'],
  ['description', 'Descripción'],
];

const defaultData = FIELD_LIST.reduce((acc, [k]) => { acc[k] = ''; return acc; }, {});

function SharkAttack(props) {
  // IMPORTANTE: la ruta usa :sharkAttackId
  const { sharkAttackId } = props.match.params;
  const isEdit = sharkAttackId && sharkAttackId !== 'new';

  const dispatch = useDispatch();
  const loggedUser = useSelector(({ auth }) => auth.user);
  const orgId = loggedUser && loggedUser.selectedOrganization && loggedUser.selectedOrganization.id;

  const [form, setForm] = useState({ ...defaultData });
  const [saving, setSaving] = useState(false);

  // ---- Estado Parte 3 (relacionados) ----
  const [relatedLoading, setRelatedLoading] = useState(false);
  const [relatedError, setRelatedError] = useState('');
  const [related, setRelated] = useState([]); // 5 casos

  // LazyQuery para leer detalle cuando el efecto lo indique
  const [readSharkAttack, { data, loading }] = useLazyQuery(
    GqlGet({ id: 'placeholder', organizationId: 'placeholder' }).query,
    { fetchPolicy: 'network-only' }
  );

  // === Efecto solicitado (adaptado) ===
  useEffect(() => {
    function updateSharkAttackState() {
      const { sharkAttackId } = props.match.params;

      if (sharkAttackId !== 'new') {
        if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id !== "") {
          readSharkAttack({
            variables: {
              organizationId: loggedUser.selectedOrganization.id,
              id: sharkAttackId
            }
          });
        }
      } else if (loggedUser.selectedOrganization && loggedUser.selectedOrganization.id) {
        setForm({ ...defaultData, organizationId: loggedUser.selectedOrganization.id });
        dispatch(Actions.setSharkAttacksPage(0));
      }
    }
    updateSharkAttackState();
  }, [dispatch, props.match.params, loggedUser.selectedOrganization, readSharkAttack]);

  // Cuando llega el detalle desde la query, hidrata el formulario
  useEffect(() => {
    if (isEdit && data && data.FactsMngSharkAttack) {
      const sa = data.FactsMngSharkAttack;
      const next = FIELD_LIST.reduce((acc, [k]) => {
        acc[k] = (sa && sa[k]) || '';
        return acc;
      }, {});
      setForm(next);
    }
  }, [isEdit, data]);

  // Mutaciones
  const [doCreate] = useMutation(GqlCreate({}).mutation);
  const [doUpdate] = useMutation(GqlUpdate({}).mutation);

  function handleChange(e) {
    const { name, value } = e.target;
    setForm(prev => ({ ...prev, [name]: value }));
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setSaving(true);
    try {
      const payload = { ...form, organizationId: orgId };

      if (!isEdit) {
        const result = await doCreate({ variables: { input: payload } });
        const d = result && result.data;
        const created = d && d.FactsMngCreateSharkAttack;
        if (created && created.id) {
          const slug = (created.name || '')
            .replace(/[\s_·!@#$%^&*(),.?":{}|<>]+/g, '-')
            .toLowerCase();
          props.history.push(`/shark-attack-mng/shark-attacks/${created.id}/${slug}`);
        }
      } else {
        const result = await doUpdate({ variables: { id: sharkAttackId, input: payload, merge: true } });
        const d = result && result.data;
        const updated = (d && d.FactsMngUpdateSharkAttack) || (d && d.FactsMngUpdateSharkAttacks);
        if (updated && updated.id) {
          const slug = (updated.name || '')
            .replace(/[\s_·!@#$%^&*(),.?":{}|<>]+/g, '-')
            .toLowerCase();
          props.history.push(`/shark-attack-mng/shark-attacks/${updated.id}/${slug}`);
        }
      }
    } finally {
      setSaving(false);
    }
  }

  // ===== Parte 3: Consulta de datos relacionados (solo frontend) =====
  async function handleConsultRelated() {
    setRelatedError('');
    setRelated([]);
    setRelatedLoading(true);

    try {
      const country = (form.country || '').trim();
      if (!country) {
        setRelatedError('Para consultar, primero indique el País en el formulario.');
        setRelatedLoading(false);
        return;
      }

      const where = encodeURIComponent("country='" + country.toUpperCase() + "'");
      const url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/global-shark-attack/records?where=" + where + "&limit=5";

      const resp = await fetch(url);
      if (!resp.ok) throw new Error('Respuesta HTTP no OK');

      const json = await resp.json();
      const results = (json && json.results) ? json.results : [];

      // Delay de 1s para apreciar el loading
      await new Promise(function (r) { setTimeout(r, 1000); });

      setRelated(results);
    } catch (err) {
      setRelatedError('No fue posible consultar casos relacionados.');
    } finally {
      setRelatedLoading(false);
    }
  }

  return (
    <div className="p-24 max-w-4xl w-full">
      <h2 className="mb-16">{!isEdit ? 'Crear Shark Attack' : 'Editar Shark Attack'}</h2>

      {isEdit && loading ? (
        <CircularProgress />
      ) : (
        <Paper className="p-16">
          <form onSubmit={handleSubmit} className="grid gap-16">
            <Grid container spacing={2}>
              {FIELD_LIST.map(([key, label]) => (
                <Grid item xs={12} sm={6} key={key}>
                  <TextField
                    label={label}
                    name={key}
                    value={form[key] || ''}
                    onChange={handleChange}
                    variant="outlined"
                    fullWidth
                  />
                </Grid>
              ))}
            </Grid>

            <div className="flex gap-12 mt-16">
              <Button type="submit" color="primary" variant="contained" disabled={saving || !orgId}>
                {saving ? 'Guardando…' : !isEdit ? 'Crear' : 'Actualizar'}
              </Button>
              <Button
                variant="outlined"
                onClick={() => props.history.push('/shark-attack-mng/shark-attacks')}
              >
                Cancelar
              </Button>
            </div>

            {/* ====== Parte 3: Botón + Loading + Lista ====== */}
            <Divider className="my-16" />
            <div className="flex items-center gap-12">
              <Button
                variant="contained"
                onClick={handleConsultRelated}
                disabled={relatedLoading || !((form.country || '').trim())}
              >
                {'Consultar más casos en ' + (form.country ? form.country : 'País')}
              </Button>
              {relatedLoading && (
                <div style={{ flex: 1 }}>
                  <LinearProgress />
                </div>
              )}
            </div>

            {relatedError ? (
              <Typography color="error" className="mt-8">
                {relatedError}
              </Typography>
            ) : null}

            {related.length > 0 && (
              <div className="mt-12">
                <Typography variant="subtitle1" className="mb-8">
                  Casos relacionados:
                </Typography>
                <Paper variant="outlined">
                  <List dense>
                    {related.map((r, idx) => {
                      const line1Parts = [];
                      if (r.date) line1Parts.push('Fecha: ' + r.date);
                      if (r.country) line1Parts.push('País: ' + r.country);
                      const line1 = line1Parts.join(' · ');

                      const line2Parts = [];
                      if (r.type) line2Parts.push('Tipo: ' + r.type);
                      if (r.species) line2Parts.push('Especie: ' + r.species);
                      const line2 = line2Parts.join(' · ');

                      const keyVal = (r.original_order != null) ? String(r.original_order) : String(idx);

                      return (
                        <React.Fragment key={keyVal}>
                          <ListItem>
                            <ListItemText
                              primary={line1 || 'Caso'}
                              secondary={line2 || null}
                            />
                          </ListItem>
                          {idx < related.length - 1 && <Divider component="li" />}
                        </React.Fragment>
                      );
                    })}
                  </List>
                </Paper>
              </div>
            )}
          </form>
        </Paper>
      )}
    </div>
  );
}

export default withRouter(SharkAttack);
