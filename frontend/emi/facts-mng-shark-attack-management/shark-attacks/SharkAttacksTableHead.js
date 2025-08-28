import React, { useState } from 'react';
import { TableHead, TableSortLabel, TableCell, TableRow, Checkbox, Tooltip, IconButton, Icon, Menu, MenuList, MenuItem, ListItemIcon, ListItemText, } from '@material-ui/core';
import { Button, DialogTitle, DialogContent, DialogContentText, DialogActions } from '@material-ui/core';
import clsx from 'clsx';
import { useDispatch, useSelector } from 'react-redux';
import * as Actions from 'app/store/actions';
import { makeStyles } from '@material-ui/styles';

const useStyles = makeStyles(theme => ({
  actionsButtonWrapper: {
    background: theme.palette.background.paper
  }
}));

function SharkAttacksTableHead(props) {
  const dispatch = useDispatch();
  const order = useSelector(({ SharkAttackManagement }) => SharkAttackManagement.sharkAttacks.order);
  const classes = useStyles(props);
  const [selectedSharkAttacksMenu, setSelectedSharkAttacksMenu] = useState(null);

  const rows = [
    { id: 'date', align: 'left', disablePadding: false, label: 'Fecha', sort: true },
    { id: 'country', align: 'left', disablePadding: false, label: 'País', sort: true },
    { id: 'type', align: 'left', disablePadding: false, label: 'Tipo', sort: true },
    { id: 'species', align: 'left', disablePadding: false, label: 'Especie', sort: true },
  ];

  const createSortHandler = property => event => {
    props.onRequestSort(event, property);
  };

  const removeHandler = () => {
    props.onRequestRemove();
  };

  function openSelectedSharkAttacksMenu(event) {
    setSelectedSharkAttacksMenu(event.currentTarget);
  }

  function closeSelectedSharkAttacksMenu() {
    setSelectedSharkAttacksMenu(null);
  }

  return (
    <TableHead>
      <TableRow className="h-64">
        <TableCell padding="checkbox" className="relative pl-4 sm:pl-12">
          <Checkbox
            indeterminate={props.numSelected > 0 && props.numSelected < props.rowCount}
            checked={props.numSelected === props.rowCount}
            onChange={props.onSelectAllClick}
          />
          {props.numSelected > 0 && (
            <div className={clsx("flex items-center justify-center absolute w-64 top-0 left-0 ml-68 h-64 z-10", classes.actionsButtonWrapper)}>
              <IconButton
                aria-owns={selectedSharkAttacksMenu ? 'selectedSharkAttacksMenu' : null}
                aria-haspopup="true"
                onClick={openSelectedSharkAttacksMenu}
              >
                <Icon>more_horiz</Icon>
              </IconButton>
              <Menu
                id="selectedSharkAttacksMenu"
                anchorEl={selectedSharkAttacksMenu}
                open={Boolean(selectedSharkAttacksMenu)}
                onClose={closeSelectedSharkAttacksMenu}
              >
                <MenuList>
                  <MenuItem
                    onClick={() => dispatch(Actions.openDialog({
                      children: (
                        <React.Fragment>
                          <DialogTitle id="alert-dialog-title">Eliminar</DialogTitle>
                          <DialogContent>
                            <DialogContentText id="alert-dialog-description">
                              ¿Seguro que deseas eliminar los elementos seleccionados?
                            </DialogContentText>
                          </DialogContent>
                          <DialogActions>
                            <Button onClick={() => { dispatch(Actions.closeDialog()); closeSelectedSharkAttacksMenu() }} color="primary">
                              No
                            </Button>
                            <Button onClick={() => { dispatch(Actions.closeDialog()); closeSelectedSharkAttacksMenu(); removeHandler() }} color="primary" autoFocus>
                              Sí, eliminar
                            </Button>
                          </DialogActions>
                        </React.Fragment>
                      )
                    }))}
                  >
                    <ListItemIcon className="min-w-40">
                      <Icon>delete</Icon>
                    </ListItemIcon>
                    <ListItemText primary="Eliminar" />
                  </MenuItem>
                </MenuList>
              </Menu>
            </div>
          )}
        </TableCell>
        {rows.map(row => {
          return (
            <TableCell
              key={row.id}
              align={row.align}
              padding={row.disablePadding ? 'none' : 'default'}
              sortDirection={order.id === row.id ? order.direction : false}
            >
              {row.sort ? (
                <Tooltip
                  title="Sort"
                  placement={row.align === "right" ? 'bottom-end' : 'bottom-start'}
                  enterDelay={300}
                >
                  <TableSortLabel
                    active={order.id === row.id}
                    direction={order.direction}
                    onClick={createSortHandler(row.id)}
                  >
                    {row.label}
                  </TableSortLabel>
                </Tooltip>
              ) : (
                row.label
              )}
            </TableCell>
          );
        })}
      </TableRow>
    </TableHead>
  );
}

export default SharkAttacksTableHead;
