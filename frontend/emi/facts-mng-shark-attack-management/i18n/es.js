export default {
  navigation: {
    'settings': 'Configuraciones',
    'facts-mng-shark-attack-management': 'SharkAttacks',
  },
  shark_attacks: {
    shark_attacks: 'SharkAttacks',
    search: 'Búsqueda rápida por nombre',
    add_new_shark_attack: 'Agregar Nueva',
    add_new_shark_attack_short: 'Agregar',
    rows_per_page: 'Filas por página:',
    of: 'de',
    remove: 'Eliminar',
    table_colums: {
      name: 'Nombre',
      active: 'Activo'
    },
    remove_dialog_title: "¿Desea eliminar las sharkAttacks seleccionadas?",
    remove_dialog_description: "Esta acción no se puede deshacer",
    remove_dialog_no: "No",
    remove_dialog_yes: "Si",
    filters: {
      title: "Filtros",
      active: "Activo"
    }
  },
  shark_attack: {
    shark_attacks: 'SharkAttacks',
    shark_attack_detail: 'Detalle de la SharkAttack',
    save: 'GUARDAR',
    basic_info: 'Información Básica',
    name: 'Nombre',
    description: 'Descripción',
    active: 'Activo',
    metadata_tab: 'Metadatos',
    metadata: {
      createdBy: 'Creado por',
      createdAt: 'Creado el',
      updatedBy: 'Modificado por',
      updatedAt: 'Modificado el',
    },
    not_found: 'Lo sentimos pero no pudimos encontrar la entidad que busca',
    internal_server_error: 'Error Interno del Servidor',
    update_success: 'SharkAttack ha sido actualizado',
    create_success: 'SharkAttack ha sido creado',
    form_validations: {
      name: {
        length: "El nombre debe tener al menos {len} caracteres",
        required: "El nombre es requerido",
      }
    },
  }
};