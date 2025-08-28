import React, { useEffect, useState } from 'react';
import { TextField, Button, CircularProgress, Paper, Grid } from '@material-ui/core';
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
          </form>
        </Paper>
      )}
    </div>
  );
}

export default withRouter(SharkAttack);
