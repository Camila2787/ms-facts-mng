import React, { useEffect, useState } from 'react';
import { Table, TableBody, TableCell, TablePagination, TableRow, Checkbox } from '@material-ui/core';
import { FuseScrollbars } from '@fuse';
import { withRouter } from 'react-router-dom';
import SharkAttacksTableHead from './SharkAttacksTableHead';
import * as Actions from '../store/actions';
import { useDispatch, useSelector } from 'react-redux';
import { useSubscription, useMutation } from "@apollo/react-hooks";
import { MDText } from 'i18n-react';
import i18n from "../i18n";
import { onFactsMngSharkAttackModified, importSharkAttacks as importSharkAttacksGql } from "../gql/SharkAttack";

function SharkAttacksTable(props) {
  const dispatch = useDispatch();
  const sharkAttacks = useSelector(({ SharkAttackManagement }) => SharkAttackManagement.sharkAttacks.data);
  const { filters, rowsPerPage, page, order, totalDataCount } =
    useSelector(({ SharkAttackManagement }) => SharkAttackManagement.sharkAttacks);
  const user = useSelector(({ auth }) => auth.user);

  const [selected, setSelected] = useState([]);
  const [isImporting, setIsImporting] = useState(false);

  const T = new MDText(i18n.get(user.locale));

  const onFactsMngSharkAttackModifiedData =
    useSubscription(...onFactsMngSharkAttackModified({ id: "ANY" }));

  const [doImportSharks] = useMutation(importSharkAttacksGql().mutation);

  useEffect(() => {
    dispatch(Actions.setSharkAttacksFilterOrganizationId(user.selectedOrganization.id));
  }, [user.selectedOrganization]);

  useEffect(() => {
    if (filters.organizationId) {
      dispatch(Actions.getSharkAttacks({ filters, order, page, rowsPerPage }));
    }
  }, [dispatch, filters, order, page, rowsPerPage, onFactsMngSharkAttackModifiedData.data]);

  async function handleImportClick() {
    try {
      setIsImporting(true);
      await doImportSharks({ fetchPolicy: 'no-cache' });
      await dispatch(Actions.getSharkAttacks({ filters, order, page, rowsPerPage }));
    } finally {
      setIsImporting(false);
    }
  }

  function handleRequestSort(event, property) {
    const id = property;
    let direction = 'desc';
    if (order.id === property && order.direction === 'desc') {
      direction = 'asc';
    }
    dispatch(Actions.setSharkAttacksOrder({ direction, id }));
  }

  function handleRequestRemove() {
    dispatch(Actions.removeSharkAttacks(selected, { filters, order, page, rowsPerPage }));
  }

  function handleSelectAllClick(event) {
    if (event.target.checked) {
      setSelected(sharkAttacks.map(n => n.id));
      return;
    }
    setSelected([]);
  }

  function handleClick(item) {
    const slug = (item.species || item.type || item.country || 'detalle')
      .toString()
      .replace(/[\s_·!@#$%^&*(),.?":{}|<>]+/g, '-')
      .toLowerCase();

    props.history.push('/shark-attack-mng/shark-attacks/' + item.id + '/' + slug);
  }

  function handleCheck(event, id) {
    const selectedIndex = selected.indexOf(id);
    let newSelected = [];

    if (selectedIndex === -1) {
      newSelected = newSelected.concat(selected, id);
    } else if (selectedIndex === 0) {
      newSelected = newSelected.concat(selected.slice(1));
    } else if (selectedIndex === selected.length - 1) {
      newSelected = newSelected.concat(selected.slice(0, -1));
    } else if (selectedIndex > 0) {
      newSelected = newSelected.concat(
        selected.slice(0, selectedIndex),
        selected.slice(selectedIndex + 1)
      );
    }
    setSelected(newSelected);
  }

  function handleChangePage(event, page) {
    dispatch(Actions.setSharkAttacksPage(page));
  }

  function handleChangeRowsPerPage(event) {
    dispatch(Actions.setSharkAttacksRowsPerPage(event.target.value));
  }

  return (
    <div className="w-full flex flex-col">

      <div className="w-full flex items-center justify-end mb-12 px-12">
        <button
          className="px-16 py-8 rounded bg-blue text-white"
          onClick={handleImportClick}
          disabled={isImporting}
          aria-label="Importar ataques"
          title="Importar 100 casos desde OpenDataSoft"
        >
          {isImporting ? 'IMPORTANDO…' : 'IMPORTAR'}
        </button>
      </div>

      <FuseScrollbars className="flex-grow overflow-x-auto">
        <Table className="min-w-xs" aria-labelledby="tableTitle">

          <SharkAttacksTableHead
            numSelected={selected.length}
            order={order}
            onSelectAllClick={handleSelectAllClick}
            onRequestSort={handleRequestSort}
            onRequestRemove={handleRequestRemove}
            rowCount={sharkAttacks.length}
          />

          <TableBody>
            {sharkAttacks.map(n => {
              const isSelected = selected.indexOf(n.id) !== -1;
              return (
                <TableRow
                  className="h-64 cursor-pointer"
                  hover
                  role="checkbox"
                  aria-checked={isSelected}
                  tabIndex={-1}
                  key={n.id}
                  selected={isSelected}
                  onClick={event => handleClick(n)}
                >
                  <TableCell className="w-48 px-4 sm:px-12" padding="checkbox">
                    <Checkbox
                      checked={isSelected}
                      onClick={event => event.stopPropagation()}
                      onChange={event => handleCheck(event, n.id)}
                    />
                  </TableCell>

                  <TableCell component="th" scope="row">{n.date || ''}</TableCell>
                  <TableCell>{n.country || ''}</TableCell>
                  <TableCell>{n.type || ''}</TableCell>
                  <TableCell>{n.species || ''}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </FuseScrollbars>

      <TablePagination
        component="div"
        count={totalDataCount}
        rowsPerPage={rowsPerPage}
        page={page}
        backIconButtonProps={{ 'aria-label': 'Previous Page' }}
        nextIconButtonProps={{ 'aria-label': 'Next Page' }}
        onChangePage={handleChangePage}
        onChangeRowsPerPage={handleChangeRowsPerPage}
        labelRowsPerPage={T.translate("shark_attacks.rows_per_page")}
        labelDisplayedRows={({ from, to, count }) =>
          `${from}-${to === -1 ? count : to} ${T.translate("shark_attacks.of")} ${count}`
        }
      />
    </div>
  );
}

export default withRouter(SharkAttacksTable);
