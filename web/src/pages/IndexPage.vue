<template>
  <q-page class="flex">
    <q-card style="width: 100%">
      <q-tabs
        v-model="tab"
        inline-label
        class="bg-grey-1 text-grey-10 shadow-2"
        align="left"
      >
        <q-tab name="console" icon="build" label="控制台" />
      </q-tabs>
      <q-separator />

      <q-tab-panels v-model="tab" animated>
        <q-tab-panel name="console">
          <q-splitter v-model="splitterModel" class="p-page-height">
            <template v-slot:before>
              <div id="editor" class="p-editor"></div>
            </template>

            <template v-slot:after>
              <div id="result-editor" class="p-editor"></div>
            </template>
          </q-splitter>
        </q-tab-panel>
      </q-tab-panels>
    </q-card>
  </q-page>
</template>

<script>
import { defineComponent } from "vue";
import { ref } from "vue";
import ace from "ace-builds";
import "ace-builds/src-noconflict/mode-sql";
import "ace-builds/src-noconflict/ext-searchbox";
import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-noconflict/ext-beautify";

import "ace-builds/src-noconflict/snippets/sql";
import "ace-builds/src-noconflict/snippets/mysql";
import "ace-builds/src-noconflict/mode-json";
import { api } from "../boot/axios";
import { useQuasar } from "quasar";

export default defineComponent({
  name: "DeveloperTool",
  setup() {
    const $q = useQuasar();

    function alert(title, message) {
      $q.dialog({
        title: title,
        message: message,
      })
        .onOk(() => {
          // console.log('OK')
        })
        .onCancel(() => {
          // console.log('Cancel')
        })
        .onDismiss(() => {
          // console.log('I am triggered on both OK and Cancel')
        });
    }
    return {
      alert,
      splitterModel: ref(40), // start at 50%
      tab: "console",
    };
  },
  mounted() {
    this.initEditor();
  },
  data() {
    return {
      editor: null,
      resultEditor: null,
      rs: null,
    };
  },
  methods: {
    initEditor() {
      this.editor = ace.edit("editor", {
        mode: "ace/mode/sql",
        enableBasicAutocompletion: true,
        enableSnippets: true,
        enableLiveAutocompletion: true,
      });
      const that = this;
      this.editor.commands.addCommand({
        name: "myCommand",
        bindKey: { win: "Ctrl-Enter", mac: "Command-Enter" },
        exec: function (editor) {
          that.exec(editor);
        },
        readOnly: true, // false if this command should not apply in readOnly mode
      });

      this.resultEditor = ace.edit("result-editor", {
        mode: "ace/mode/json",
        enableBasicAutocompletion: true,
        enableSnippets: true,
        readOnly: true,
        enableLiveAutocompletion: true,
      });
    },
    async exec(editor) {
      try {
        const rs = await api.get("/_sql", {
          params: { query: editor.getSelectedText() },
        });
        this.resultEditor.setValue(JSON.stringify(rs.data));
        const beautify = ace.require("ace/ext/beautify");
        beautify.beautify(this.resultEditor.getSession());
      } catch (e) {
        this.alert("Error", e.message);
      }
    },
  },
});
</script>

<style lang="scss">
.p-editor {
  height: calc(100vh - 140px);
}
</style>
